module FakeDynamo
  class PrimaryKey

    include ActiveModel::Validations
    include Validation

    attr_accessor :hash_key, :range_key
    validates_presence_of :hash_key

    def initialize(data)
      extract_values(data)
      validate!
    end

    def description
      description = { 'HashKeyElement' => hash_key.description }
      if range_key
        description['RangeKeyElement'] = range_key.description
      end
    end

    private
    def extract_values(data)
      raise ValidationException, "'KeySchema' param is required" unless data
      @hash_key = Attribute.from_data(data['HashKeyElement'])
      if range_key_element = data['RangeKeyElement']
        @range_key = Attribute.from_data(range_key_element)
      end
    end
  end
end
