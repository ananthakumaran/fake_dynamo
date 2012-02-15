module FakeDynamo
  class KeySchema

    attr_accessor :hash_key, :range_key

    def initialize(data)
      extract_values(data)
    end

    def description
      description = { 'HashKeyElement' => hash_key.description }
      if range_key
        description['RangeKeyElement'] = range_key.description
      end
    end

    private
    def extract_values(data)
      @hash_key = Attribute.from_data(data['HashKeyElement'])
      if range_key_element = data['RangeKeyElement']
        @range_key = Attribute.from_data(range_key_element)
      end
    end
  end
end
