module FakeDynamo
  class Table

    attr_accessor :creation_date_time, :read_capacity_units, :write_capacity_units,
                  :name, :status, :primary_key, :items, :size_bytes

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
