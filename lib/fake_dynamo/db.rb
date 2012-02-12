module FakeDynamo
  class DB

    def available_operations
      %w[ CreateTable ]
    end

    def process(operation, data)
      raise InvalidParameterValueException, "Invalid operation: #{operation}" unless available_operations.include? operation

      self.send operation, data
    end

    def tables
      @tables ||= {}
    end

    def CreateTable(data)
      table_name = data['TableName']
      raise ResourceInUseException, "Duplicate table name: #{table_name}" if tables[table_name]

      table = Table.new(data)
      tables[table_name] = table
      table.description
    end
  end
end
