module FakeDynamo
  class DB
    include Validation

    attr_accessor :tables

    class << self
      def instance
        @db ||= DB.new
      end
    end

    def initialize
      @tables = {}
    end

    def reset
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
      response = table.description
      table.activate
      response
    end

    def describe_table(data)
      table = find_table(data['TableName'])
      table.describe_table
    end

    def delete_table(data)
      table_name = data['TableName']
      table = find_table(table_name)
      tables.delete(table_name)
      table.delete
    end

    def list_tables(data)
      start_table = data['ExclusiveStartTableName']
      limit = data['Limit']

      all_tables = tables.keys
      start = 0

      if start_table
        if i = all_tables.index(start_table)
          start = i + 1
        end
      end

      limit ||= all_tables.size
      result_tables = all_tables[start, limit]
      response = { 'TableNames' => result_tables }

      if (start + limit) < all_tables.size
        last_table = all_tables[start + limit - 1]
        response.merge!({ 'LastEvaluatedTableName' => last_table })
      end
      response
    end

    def update_table(data)
      table = find_table(data['TableName'])
      table.update(data['ProvisionedThroughput']['ReadCapacityUnits'], data['ProvisionedThroughput']['WriteCapacityUnits'])
    end

    def self.delegate_to_table(*methods)
      methods.each do |method|
        define_method(method) do |data|
          find_table(data['TableName']).send(method, data)
        end
      end
    end

    delegate_to_table :put_item, :get_item, :delete_item, :update_item, :query, :scan

    def batch_get_item(data)
      response = {}
      consumed_capacity = {}
      unprocessed_keys = {}
      sack = Sack.new

      data['RequestItems'].each do |table_name, table_data|
        table = find_table(table_name)

        unless response[table_name]
          response[table_name] = []
          set_consumed_capacity(consumed_capacity, table, data)
        end

        table_data['Keys'].each do |key|
          if sack.has_space?
            if item_hash = table.get_raw_item(key, table_data['AttributesToGet'])
              response[table_name] << item_hash
              sack.add(item_hash)
            end
          else
            unless unprocessed_keys[table_name]
              unprocessed_keys[table_name] = {'Keys' => []}
              unprocessed_keys[table_name]['AttributesToGet'] = table_data['AttributesToGet'] if table_data['AttributesToGet']
            end

            unprocessed_keys[table_name]['Keys'] << key
          end
        end
      end

      response = { 'Responses' => response, 'UnprocessedKeys' => unprocessed_keys }
      merge_consumed_capacity(consumed_capacity, response)
    end

    def batch_write_item(data)
      response = {}
      consumed_capacity = {}
      item_collection_metrics = {}
      merge_metrics = false
      items = {}
      request_count = 0

      # validation
      data['RequestItems'].each do |table_name, requests|
        table = find_table(table_name)

        items[table.name] ||= {}
        item_collection_metrics[table.name] ||= []

        requests.each do |request|
          if request['PutRequest']
            item = table.batch_put_request(request['PutRequest'])
            check_item_conflict(items, table.name, item.key)
            items[table.name][item.key] = item
          else
            key = table.batch_delete_request(request['DeleteRequest'])
            check_item_conflict(items, table.name, key)
            items[table.name][key] = :delete
          end

          request_count += 1
        end
      end

      check_max_request(request_count)

      # real modification
      items.each do |table_name, requests|
        table = find_table(table_name)
        item_collection_metrics[table.name] ||= []

        requests.each do |key, value|
          if value == :delete
            table.batch_delete(key)
          else
            table.batch_put(value)
          end

          unless (metrics = Item.from_key(key).collection_metrics(data)).empty?
            merge_metrics = true
            item_collection_metrics[table.name] << metrics['ItemCollectionMetrics']
          end

        end
        set_consumed_capacity(consumed_capacity, table, data)
      end

      response = { 'UnprocessedItems' => {} }
      response = merge_consumed_capacity(consumed_capacity, response)
      if merge_metrics
        response.merge!({'ItemCollectionMetrics' => item_collection_metrics})
      end
      response
    end

    private

    def set_consumed_capacity(consumed_capacity, table, data)
      unless (capacity = table.consumed_capacity(data)).empty?
        consumed_capacity[table.name] = capacity['ConsumedCapacity']
      end
    end

    def merge_consumed_capacity(consumed_capacity, response)
      unless consumed_capacity.empty?
        response['ConsumedCapacity'] = consumed_capacity.values
      end
      response
    end

    def check_item_conflict(items, table_name, key)
      if items[table_name][key]
        raise ValidationException, 'Provided list of item keys contains duplicates'
      end
    end


    def find_table(table_name)
      tables[table_name] or raise ResourceNotFoundException, "Table: #{table_name} not found"
    end

    def check_max_request(count)
      if count > 25
        raise ValidationException, 'Too many items requested for the BatchWriteItem call'
      end
    end

  end
end
