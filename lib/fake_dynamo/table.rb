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


    def find_item()
    end

    def put_item(data)
      item = Item.new(data['Item'], key_schema)
      old_item = @items[item.key]
      check_conditions(old_item, data['Expected'], item)
      @items[item.key] = item
    end

    def check_conditions(old_item, conditions, item)
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
            raise ConditionalCheckFailedException unless old_item and old_item[name] == expected_attr
          elsif !exist # false
            raise ValidationException, "Cannot expect an attribute to have a specified value while expecting it to not exist"
          end
        end
      end
    end


    private
    def init
      @creation_date_time = Time.now.to_i
      @status = 'ACTIVE'
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
