module FakeDynamo
  class Table
    include Validation
    include Filter

    attr_accessor :creation_date_time, :read_capacity_units, :write_capacity_units,
                  :name, :status, :attribute_definitions, :key_schema, :items, :size_bytes,
                  :local_secondary_indexes, :last_increased_time, :last_decreased_time

    def initialize(data)
      extract_values(data)
      init
    end

    def description
      {
        'TableDescription' => {
          'AttributeDefinitions' => attribute_definitions.map(&:description),
          'CreationDateTime' => creation_date_time,
          'KeySchema' => key_schema.description,
          'ProvisionedThroughput' => throughput_description,
          'TableName' => name,
          'TableStatus' => status,
          'ItemCount' => items.count,
          'TableSizeBytes' => size_bytes
        }.merge(local_secondary_indexes_description)
      }
    end

    def throughput_description
      result = {
        'NumberOfDecreasesToday' => 0,
        'ReadCapacityUnits' => read_capacity_units,
        'WriteCapacityUnits' => write_capacity_units
      }

      if last_increased_time
        result['LastIncreaseDateTime'] = @last_increased_time
      end

      if last_decreased_time
        result['LastDecreaseDateTime'] = @last_decreased_time
      end

      result
    end

    def local_secondary_indexes_description
      if local_secondary_indexes
        { 'LocalSecondaryIndexes' => local_secondary_indexes.map(&:description) }
      else
        {}
      end
    end

    def create_table_data
      {
        'TableName' => name,
        'AttributeDefinitions' => attribute_definitions.map(&:description),
        'KeySchema' => key_schema.description,
        'ProvisionedThroughput' => {
          'ReadCapacityUnits' => read_capacity_units,
          'WriteCapacityUnits' => write_capacity_units
        }
      }
    end

    def put_item_data(item)
      {
        'TableName' => name,
        'Item' => item.as_hash
      }
    end

    def describe_table
      { 'Table' => description['TableDescription'] }
    end

    def activate
      @status = 'ACTIVE'
    end

    def delete
      @status = 'DELETING'
      description
    end

    def update(read_capacity_units, write_capacity_units)
      if @read_capacity_units > read_capacity_units
        @last_decreased_time = Time.now.to_i
      elsif @read_capacity_units < read_capacity_units
        @last_increased_time = Time.now.to_i
      end

      if @write_capacity_units > write_capacity_units
        @last_decreased_time = Time.now.to_i
      elsif @write_capacity_units < write_capacity_units
        @last_increased_time = Time.now.to_i
      end

      @read_capacity_units, @write_capacity_units = read_capacity_units, write_capacity_units

      response = description
      response['TableDescription']['TableStatus'] = 'UPDATING'
      response
    end

    def put_item(data)
      item = Item.from_data(data['Item'], key_schema, attribute_definitions)
      old_item = @items[item.key]
      check_conditions(old_item, data['Expected'])
      @items[item.key] = item

      return_values(data, old_item).merge(item.collection_metrics(data))
    end

    def batch_put_request(data)
      Item.from_data(data['Item'], key_schema, attribute_definitions)
    end

    def batch_put(item)
      @items[item.key] = item
    end

    def get_item(data)
      response = consumed_capacity(data)
      if item_hash = get_raw_item(data['Key'], data['AttributesToGet'])
        response.merge!('Item' => item_hash)
      end
      response
    end

    def get_raw_item(key_data, attributes_to_get)
      key = Key.from_data(key_data, key_schema)
      item = @items[key]

      if item
        filter_attributes(item, attributes_to_get)
      end
    end

    def filter_attributes(item, attributes_to_get)
      hash = item.as_hash
      if attributes_to_get
        hash.select! do |attribute, value|
          attributes_to_get.include? attribute
        end
      end
      hash
    end

    def delete_item(data)
      key = Key.from_data(data['Key'], key_schema)
      item = @items[key]
      check_conditions(item, data['Expected'])

      @items.delete(key) if item
      if !item
        item = Item.from_key(key)
        consumed_capacity(data).merge(item.collection_metrics(data))
      else
        return_values(data, item).merge(consumed_capacity(data)).merge(item.collection_metrics(data))
      end
    end

    def batch_delete_request(data)
      Key.from_data(data['Key'], key_schema)
    end

    def batch_delete(key)
      @items.delete(key)
    end

    def update_item(data)
      key = Key.from_data(data['Key'], key_schema)
      item = @items[key]
      check_conditions(item, data['Expected'])

      unless item
        item = Item.from_key(key)
        if create_item?(data)
          @items[key] = item
        else
          return consumed_capacity(data).merge(item.collection_metrics(data))
        end
        item_created = true
      end

      old_item = deep_copy(item)
      begin
        old_hash = item.as_hash
        if attribute_updates = data['AttributeUpdates']
          attribute_updates.each do |name, update_data|
            item.update(name, update_data)
          end

          item.validate_attribute_types(attribute_definitions)
        end
      rescue => e
        if item_created
          @items.delete(key)
        else
          @items[key] = old_item
        end
        raise e
      end

      return_values(data, old_hash, item).merge(item.collection_metrics(data))
    end

    def deep_copy(x)
      Marshal.load(Marshal.dump(x))
    end

    def query(data)
      range_key_present
      select_and_attributes_to_get_present?(data)
      validate_limit(data)

      index = nil
      if index_name = data['IndexName']
        index = local_secondary_indexes.find { |i| i.name == index_name }
        raise ValidationException, "The provided starting key is invalid" unless index
        schema = index.key_schema
      else
        schema = key_schema
      end

      hash_condition = data['KeyConditions'][schema.hash_key.name]
      validate_hash_condition(hash_condition)

      hash_attribute = Attribute.from_hash(schema.hash_key.name, hash_condition['AttributeValueList'].first)
      matched_items = get_items_by_hash_key(hash_attribute)

      forward = data.has_key?('ScanIndexForward') ? data['ScanIndexForward'] : true
      if index
        matched_items = drop_till_start_index(matched_items, data['ExclusiveStartKey'], forward, schema)
      else
        matched_items = drop_till_start(matched_items, data['ExclusiveStartKey'], forward, schema)
      end

      if !(range_condition = data['KeyConditions'].clone.tap { |h| h.delete(schema.hash_key.name) }).empty?
        validate_range_condition(range_condition, schema)
        conditions = range_condition
      else
        conditions = {}
      end

      results, last_evaluated_item, _ = filter(matched_items, conditions, data['Limit'], true, sack_attributes(data, index))

      response = {'Count' => results.size}.merge(consumed_capacity(data))
      merge_items(response, data, results, index)

      if last_evaluated_item
        if index
          response['LastEvaluatedKey'] = Key.from_index_item(last_evaluated_item, schema).as_hash
        else
          response['LastEvaluatedKey'] = last_evaluated_item.key.as_hash
        end
      end
      response
    end

    def scan(data)
      select_and_attributes_to_get_present?(data)
      total_segments_and_segment_present?(data)
      validate_limit(data)

      conditions = data['ScanFilter'] || {}


      if (segment = data['Segment']) && (total_segments = data['TotalSegments'])
        chunk_size = (items.values.size / total_segments.to_f).ceil
        current_segment = items.values.slice(segment * chunk_size, chunk_size) || []
      else
        current_segment = items.values
      end

      all_items = drop_till_start(current_segment, data['ExclusiveStartKey'], true, key_schema)
      results, last_evaluated_item, scaned_count = filter(all_items, conditions, data['Limit'], false)
      response = {
        'Count' => results.size,
        'ScannedCount' => scaned_count}.merge(consumed_capacity(data))

      merge_items(response, data, results)

      if last_evaluated_item
        response['LastEvaluatedKey'] = last_evaluated_item.key.as_hash
      end

      response
    end

    def merge_items(response, data, results, index = nil)
      if (attrs = attributes_to_get(data, index)) != false
        response['Items'] = results.map { |r| filter_attributes(r, attrs) }
      end
      response
    end

    def attributes_to_get(data, index)
      if data['Select'] != 'COUNT'
        if index
          attributes_to_get = projected_attributes(index)
        else
          attributes_to_get = nil # select everything
        end


        if data['AttributesToGet']
          attributes_to_get = data['AttributesToGet']
        elsif data['Select'] == 'ALL_PROJECTED_ATTRIBUTES'
          attributes_to_get = projected_attributes(index)
        elsif data['Select'] == 'ALL_ATTRIBUTES'
          attributes_to_get = nil
        end
      else
        false
      end
    end

    def sack_attributes(data, index)
      return if !index || index.projection.type == 'ALL'

      if data['Select'] == 'COUNT'
        return projected_attributes(index)
      end

      if attrs = attributes_to_get(data, index)
        if (attrs - (projected_attributes(index))).empty?
          return projected_attributes(index)
        end
      end
    end

    def projected_attributes(index)
      if !index
        raise ValidationException, "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
      else
        case index.projection.type
        when 'ALL'
          nil
        when 'KEYS_ONLY'
          (key_schema.keys + index.key_schema.keys).uniq
        when 'INCLUDE'
          (key_schema.keys + index.key_schema.keys + index.projection.non_key_attributes).uniq
        end
      end
    end

    def select_and_attributes_to_get_present?(data)
      select = data['Select']
      if select and data['AttributesToGet'] and (select != 'SPECIFIC_ATTRIBUTES')
        raise ValidationException, "Cannot specify the AttributesToGet when choosing to get only the #{select}"
      end
    end

    def total_segments_and_segment_present?(data)
      segment, total_segments = data['Segment'], data['TotalSegments']

      if (total_segments && !segment)
        raise ValidationException, "The Segment parameter is required but was not present in the request when parameter TotalSegments is present"
      end

      if (segment && !total_segments)
        raise ValidationException, "The TotalSegments parameter is required but was not present in the request when Segment parameter is present"
      end

      if (segment && total_segments) &&
          (segment >= total_segments)
        raise ValidationException, "The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: #{segment} is not less than TotalSegments: #{total_segments}"
      end
    end

    def validate_limit(data)
      if data['Limit'] and data['Limit'] <= 0
        raise ValidationException, "Limit failed to satisfy constraint: Member must have value greater than or equal to 1"
      end
    end

    def drop_till_start(all_items, start_key_hash, forward, schema)
      all_items = all_items.sort_by { |item| item.key }

      unless forward
        all_items = all_items.reverse
      end

      if start_key_hash
        start_key = Key.from_data(start_key_hash, schema)
        all_items.drop_while do |item|
          if forward
            item.key <= start_key
          else
            item.key >= start_key
          end
        end
      else
        all_items
      end
    end

    def drop_till_start_index(all_items, start_key_hash, forward, schema)
      all_items = all_items.sort_by { |item| Key.from_index_item(item, schema) }

      unless forward
        all_items = all_items.reverse
      end

      if start_key_hash
        start_key = Key.from_index_schema(start_key_hash, schema, key_schema)
        all_items.drop_while do |item|
          if forward
            Key.from_index_item(item, schema) <= start_key
          else
            Key.from_index_item(item, schema) >= start_key
          end
        end
      else
        all_items
      end
    end

    def filter(items, conditions, limit, fail_on_type_mismatch, sack_attributes = nil)
      limit ||= -1
      result = []
      sack = Sack.new
      last_evaluated_item = nil
      scaned_count = 0
      items.each do |item|
        select = true
        conditions.each do |attribute_name, condition|
          value = condition['AttributeValueList']
          comparison_op = condition['ComparisonOperator']
          unless self.send("#{comparison_op.downcase}_filter", value, item[attribute_name], fail_on_type_mismatch)
            select = false
            break
          end
        end

        scaned_count += 1

        if select
          result << item
          if sack_attributes
            sack.add(filter_attributes(item, sack_attributes))
          else
            sack.add(item)
          end

          if (limit -= 1) == 0 || (!sack.has_space?)
            last_evaluated_item = item
            break
          end
        end
      end
      [result, last_evaluated_item, scaned_count]
    end

    def get_items_by_hash_key(hash_key)
      items.values.select do |i|
        i.key.primary == hash_key
      end
    end

    def create_item?(data)
      if attribute_updates = data['AttributeUpdates']
        attribute_updates.any? do |name, update_data|
          action = update_data['Action']
          ['PUT', 'ADD', nil].include? action
        end
      else
        true
      end
    end

    def updated_attributes(data)
      data['AttributeUpdates'].map { |name, _| name }
    end

    def return_values(data, old_item, new_item={})
      old_item ||= {}
      old_hash = old_item.kind_of?(Item) ? old_item.as_hash : old_item

      new_item ||= {}
      new_hash = new_item.kind_of?(Item) ? new_item.as_hash : new_item


      return_value = data['ReturnValues']
      result = case return_value
               when 'ALL_OLD'
                 old_hash
               when 'ALL_NEW'
                 new_hash
               when 'UPDATED_OLD'
                 updated = updated_attributes(data)
                 old_hash.select { |name, _| updated.include? name }
               when 'UPDATED_NEW'
                 updated = updated_attributes(data)
                 new_hash.select { |name, _| updated.include? name }
               when 'NONE', nil
                 {}
               else
                 raise 'unknown return value'
               end

      result = unless result.empty?
                 { 'Attributes' => result }
               else
                 {}
               end

      result.merge(consumed_capacity(data))
    end

    def consumed_capacity(data)
      if data['ReturnConsumedCapacity'] == 'TOTAL'
        {'ConsumedCapacity' => { 'CapacityUnits' => 1, 'TableName' => @name }}
      else
        {}
      end
    end

    def check_conditions(old_item, conditions)
      return unless conditions

      conditions.each do |name, predicate|
        exist = predicate['Exists']
        value = predicate['Value']

        if not value
          if exist.nil?
            raise ValidationException, "'Exists' is set to null. 'Exists' must be set to false when no Attribute value is specified"
          elsif exist
            raise ValidationException, "'Exists' is set to true. 'Exists' must be set to false when no Attribute value is specified"
          elsif !exist # false
            if old_item and old_item[name]
              raise ConditionalCheckFailedException
            end
          end
        else
          expected_attr = Attribute.from_hash(name, value)

          if exist.nil? or exist
            raise ConditionalCheckFailedException unless (old_item and old_item[name] == expected_attr)
          elsif !exist # false
            raise ValidationException, "Cannot expect an attribute to have a specified value while expecting it to not exist"
          end
        end
      end
    end


    private
    def init
      @creation_date_time = Time.now.to_i
      @status = 'CREATING'
      @items = {}
      @size_bytes = 0
    end

    def extract_values(data)
      @name = data['TableName']
      @key_schema = KeySchema.new(data['KeySchema'], data['AttributeDefinitions'])
      set_local_secondary_indexes(data)
      @attribute_definitions = data['AttributeDefinitions'].map(&Attribute.method(:from_data))
      set_throughput(data['ProvisionedThroughput'])

      validate_attribute_definitions
    end

    def set_throughput(throughput)
      @read_capacity_units = throughput['ReadCapacityUnits']
      @write_capacity_units = throughput['WriteCapacityUnits']
    end

    def set_local_secondary_indexes(data)
      if indexes_data = data['LocalSecondaryIndexes']
        @local_secondary_indexes = indexes_data.map do |index|
          LocalSecondaryIndex.from_data(index, data['AttributeDefinitions'], @key_schema)
        end
        validate_range_key(key_schema)
        validate_index_names(@local_secondary_indexes)
      end
    end

    def validate_attribute_definitions
      attribute_keys = @attribute_definitions.map(&:name)
      used_keys = @key_schema.keys
      if @local_secondary_indexes
        used_keys += @local_secondary_indexes.map(&:key_schema).map(&:keys).flatten
      end

      used_keys.uniq!

      if used_keys.uniq.size != attribute_keys.size
        raise ValidationException, "Some AttributeDefinitions are not used AttributeDefinitions: #{attribute_keys.inspect}, keys used: #{used_keys.inspect}"
      end
    end

    def range_key_present
      unless key_schema.range_key
        raise ValidationException, "Query can be performed only on a table with a HASH,RANGE key schema"
      end
    end
  end
end
