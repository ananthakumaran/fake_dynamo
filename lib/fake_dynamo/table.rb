module FakeDynamo
  class Table
    include Validation
    include Filter

    attr_accessor :creation_date_time, :read_capacity_units, :write_capacity_units,
                  :name, :status, :key_schema, :items, :size_bytes, :last_increased_time,
                  :last_decreased_time

    def initialize(data)
      extract_values(data)
      init
    end

    def description
      {
        'TableDescription' => {
          'CreationDateTime' => creation_date_time,
          'KeySchema' => key_schema.description,
          'ProvisionedThroughput' => {
            'ReadCapacityUnits' => read_capacity_units,
            'WriteCapacityUnits' => write_capacity_units
          },
          'TableName' => name,
          'TableStatus' => status
        }
      }
    end

    def create_table_data
      {
        'TableName' => name,
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

    def size_description
      { 'ItemCount' => items.count,
        'TableSizeBytes' => size_bytes }
    end

    def describe_table
      { 'Table' => description['TableDescription'] }.merge(size_description)
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

      response = description.merge(size_description)

      if last_increased_time
        response['TableDescription']['ProvisionedThroughput']['LastIncreaseDateTime'] = @last_increased_time
      end

      if last_decreased_time
        response['TableDescription']['ProvisionedThroughput']['LastDecreaseDateTime'] = @last_decreased_time
      end

      response['TableDescription']['TableStatus'] = 'UPDATING'
      response
    end

    def put_item(data)
      item = Item.from_data(data['Item'], key_schema)
      old_item = @items[item.key]
      check_conditions(old_item, data['Expected'])
      @items[item.key] = item

      consumed_capacity.merge(return_values(data, old_item))
    end

    def get_item(data)
      response = consumed_capacity
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
      consumed_capacity.merge(return_values(data, item))
    end

    def update_item(data)
      key = Key.from_data(data['Key'], key_schema)
      item = @items[key]
      check_conditions(item, data['Expected'])

      unless item
        if create_item?(data)
          item = @items[key] = Item.from_key(key)
        else
          return consumed_capacity
        end
        item_created = true
      end

      old_item = deep_copy(item)
      begin
        old_hash = item.as_hash
        data['AttributeUpdates'].each do |name, update_data|
          item.update(name, update_data)
        end
      rescue => e
        if item_created
          @items.delete(key)
        else
          @items[key] = old_item
        end
        raise e
      end

      consumed_capacity.merge(return_values(data, old_hash, item))
    end

    def deep_copy(x)
      Marshal.load(Marshal.dump(x))
    end

    def query(data)
      unless key_schema.range_key
        raise ValidationException, "Query can be performed only on a table with a HASH,RANGE key schema"
      end

      count_and_attributes_to_get_present?(data)
      validate_limit(data)

      hash_attribute = Attribute.from_hash(key_schema.hash_key.name, data['HashKeyValue'])
      matched_items = get_items_by_hash_key(hash_attribute)


      forward = data.has_key?('ScanIndexForward') ? data['ScanIndexForward'] : true

      if forward
        matched_items.sort! { |a, b| a.key.range <=> b.key.range }
      else
        matched_items.sort! { |a, b| b.key.range <=> a.key.range }
      end

      matched_items = drop_till_start(matched_items, data['ExclusiveStartKey'])

      if data['RangeKeyCondition']
        conditions = {key_schema.range_key.name => data['RangeKeyCondition']}
      else
        conditions = {}
      end

      result, last_evaluated_item, _ = filter(matched_items, conditions, data['Limit'], true)

      response = {
        'Count' => result.size,
        'ConsumedCapacityUnits' => 1 }

      unless data['Count']
        response['Items'] = result.map { |r| filter_attributes(r, data['AttributesToGet']) }
      end

      if last_evaluated_item
        response['LastEvaluatedKey'] = last_evaluated_item.key.as_key_hash
      end
      response
    end

    def scan(data)
      count_and_attributes_to_get_present?(data)
      validate_limit(data)
      conditions = data['ScanFilter'] || {}
      all_items = drop_till_start(items.values, data['ExclusiveStartKey'])
      result, last_evaluated_item, scaned_count = filter(all_items, conditions, data['Limit'], false)
      response = {
        'Count' => result.size,
        'ScannedCount' => scaned_count,
        'ConsumedCapacityUnits' => 1 }

      unless data['Count']
        response['Items'] = result.map { |r| filter_attributes(r, data['AttributesToGet']) }
      end

      if last_evaluated_item
        response['LastEvaluatedKey'] = last_evaluated_item.key.as_key_hash
      end

      response
    end

    def count_and_attributes_to_get_present?(data)
      if data['Count'] and data['AttributesToGet']
        raise ValidationException, "Cannot specify the AttributesToGet when choosing to get only the Count"
      end
    end

    def validate_limit(data)
      if data['Limit'] and data['Limit'] <= 0
        raise ValidationException, "Limit failed to satisfy constraint: Member must have value greater than or equal to 1"
      end
    end

    def drop_till_start(all_items, start_key_hash)
      if start_key_hash
        all_items.drop_while { |i| i.key.as_key_hash != start_key_hash }.drop(1)
      else
        all_items
      end
    end

    def filter(items, conditions, limit, fail_on_type_mismatch)
      limit ||= -1
      result = []
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

        if select
          result << item
          if (limit -= 1) == 0
            last_evaluated_item = item
            break
          end
        end

        scaned_count += 1
      end
      [result, last_evaluated_item, scaned_count]
    end

    def get_items_by_hash_key(hash_key)
      items.values.select do |i|
        i.key.primary == hash_key
      end
    end

    def create_item?(data)
      data['AttributeUpdates'].any? do |name, update_data|
        action = update_data['Action']
        ['PUT', 'ADD', nil].include? action
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

      unless result.empty?
        { 'Attributes' => result }
      else
        {}
      end
    end

    def consumed_capacity
      { 'ConsumedCapacityUnits' => 1 }
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
      @key_schema = KeySchema.new(data['KeySchema'])
      set_throughput(data['ProvisionedThroughput'])
    end

    def set_throughput(throughput)
      @read_capacity_units = throughput['ReadCapacityUnits']
      @write_capacity_units = throughput['WriteCapacityUnits']
    end

  end
end
