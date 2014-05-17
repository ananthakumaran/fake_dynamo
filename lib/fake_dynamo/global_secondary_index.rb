module FakeDynamo
  class GlobalSecondaryIndex
    extend Validation
    include Throughput

    attr_accessor :name, :key_schema, :projection, :status

    def initialize
      @status = 'CREATING'
    end

    class << self
      def from_data(index_data, attribute_definitions, table_key_schema)
        index = GlobalSecondaryIndex.new
        index.name = index_data['IndexName']
        index.key_schema = KeySchema.new(index_data['KeySchema'], attribute_definitions)
        index.projection = Projection.from_data(index_data['Projection'])
        index.set_throughput(index_data['ProvisionedThroughput'])

        index
      end
    end

    def sort_value(item, table_key_schema)
      value = []
      if key_schema.range_key
        value << item[key_schema.range_key.name]
      end

      value << item[table_key_schema.hash_key.name]
      if table_key_schema.range_key
        value << table_key_schema.range_key
      end
      value
    end

    def activate
      @status = 'ACTIVE'
    end

    def updating
      @status = 'UPDATING'
    end

    def description
      { 'IndexName' => name,
        'IndexSizeBytes' => 0,
        'IndexStatus' => status,
        'ItemCount' => 0,
        'KeySchema' => key_schema.description,
        'Projection' => projection.description,
        'ProvisionedThroughput' => throughput_description }
    end
  end
end
