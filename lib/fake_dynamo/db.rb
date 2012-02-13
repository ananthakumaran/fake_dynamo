module FakeDynamo
  class DB

    include Validation

    attr_reader :tables

    def initialize
      @tables = {}
    end

    def process(operation, data)
      validate_payload(operation, data)
      operation = operation.underscore
      self.send operation, data
    end

    def create_table(data)
      table_name = data['TableName']
      raise ResourceInUseException, "Duplicate table name: #{table_name}" if tables[table_name]

      table = Table.new(data)
      tables[table_name] = table
      table.description
    end

    def describe_table(data)
      table_name = data['TableName']
      table = find_table(table_name)
      table.describe_table
    end

    private
    def find_table(table_name)
      tables[table_name] or raise ResourceNotFoundException, "Table : #{table_name} not found"
    end

  end
end
