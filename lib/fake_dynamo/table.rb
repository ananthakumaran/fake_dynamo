module FakeDynamo
  class Table

    attr_accessor :creation_date_time, :read_capacity_units, :write_capacity_units,
                  :name, :status, :primary_key, :items, :size_bytes, :last_increased_time,
                  :last_decreased_time

    def initialize(data)
      extract_values(data)
      init
    end

    def description
      {
        'TableDescription' => {
          'CreationDateTime' => creation_date_time,
          'KeySchema' => primary_key.description,
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

    private
    def init
      @creation_date_time = Time.now.to_i
      @status = 'ACTIVE'
      @items = []
      @size_bytes = 0
    end

    def extract_values(data)
      @name = data['TableName']
      @primary_key = PrimaryKey.new(data['KeySchema'])
      set_throughput(data['ProvisionedThroughput'])
    end

    def set_throughput(throughput)
      @read_capacity_units = throughput['ReadCapacityUnits']
      @write_capacity_units = throughput['WriteCapacityUnits']
    end

  end
end
