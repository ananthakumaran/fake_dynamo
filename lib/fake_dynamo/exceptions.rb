module FakeDynamo
  class Error < ::StandardError

    class_attribute :description, :type, :status

    self.type = 'com.amazon.dynamodb.v20111205'
    self.status = 400

    attr_reader :detail

    def initialize(detail='')
      @detail = detail
      super(detail)
    end

    def response
      {
        '__type' => "#{self.class.type}##{class_name}",
        'message' => "#{self.class.description}: #{@detail}"
      }
    end

    def class_name
      self.class.name.split('::').last
    end

    def status
      self.class.status
    end
  end

  class UnknownOperationException < Error
    self.type = 'com.amazon.coral.service'
    self.description = ''
  end

  class InvalidParameterValueException < Error
    self.description = 'invalid parameter'
  end

  class ResourceNotFoundException < Error
    self.description = 'Requested resource not found'
  end

  class ResourceInUseException < Error
    self.description = 'Attempt to change a resource which is still in use'
  end

  class ValidationException < Error
    self.description = 'Validation error detected'
    self.type = 'com.amazon.coral.validate'
  end

  class ConditionalCheckFailedException < Error
    self.description = 'The conditional request failed'
  end
end
