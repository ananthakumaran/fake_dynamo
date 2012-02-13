module FakeDynamo
  class Table

    include Validation

    attr_accessor :creation_date_time, :read_capacity_units, :write_capacity_units,
                  :name, :status, :primary_key, :items, :size_bytes


    validates_presence_of :creation_date_time, :read_capacity_units, :write_capacity_units,
                          :name, :status, :primary_key
    validates_format_of :name, :with => /[a-zA-Z0-9_.-]+/
    validates_length_of :name, :within => 3..255

    validates_numericality_of :read_capacity_units, :write_capacity_units

    def initialize(data)
      extract_values(data)
      init
      validate!
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
      raise ValidationException, "'ProvisionThoughput' param is required" unless throughput
      @read_capacity_units = throughput['ReadCapacityUnits']
      @write_capacity_units = throughput['WriteCapacityUnits']
    end

  end
end
