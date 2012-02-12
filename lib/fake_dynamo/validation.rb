module FakeDynamo
  module Validation

    extend ActiveSupport::Concern
    include ActiveModel::Validations

    def validate!
      if invalid?
        raise ValidationException, errors.full_messages.join(', ')
      end
    end

  end
end
