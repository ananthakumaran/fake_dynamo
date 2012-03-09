module FakeDynamo
  class Table

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

    def describe_table
      description.merge({
        'ItemCount' => items.count,
        'TableSizeBytes' => size_bytes
      })
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

      response = describe_table

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
        hash = item.as_hash
        if attributes_to_get
          hash.select! do |attribute, value|
            attributes_to_get.include? attribute
          end
        end
        hash
      end
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
      end

      old_hash = item.as_hash
      data['AttributeUpdates'].each do |name, update_data|
        item.update(name, update_data)
      end

      consumed_capacity.merge(return_values(data, old_hash, item))
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
