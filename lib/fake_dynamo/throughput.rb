module FakeDynamo
  module Throughput
    extend ActiveSupport::Concern

    included do
      attr_accessor :read_capacity_units, :write_capacity_units, :last_increased_time, :last_decreased_time
    end

    def throughput_description
      result = {
        'NumberOfDecreasesToday' => 0,
        'ReadCapacityUnits' => read_capacity_units,
        'WriteCapacityUnits' => write_capacity_units
      }

      if last_increased_time
        result['LastIncreaseDateTime'] = @last_increased_time
      end

      if last_decreased_time
        result['LastDecreaseDateTime'] = @last_decreased_time
      end

      result
    end

    def set_throughput(throughput)
      @read_capacity_units = throughput['ReadCapacityUnits']
      @write_capacity_units = throughput['WriteCapacityUnits']
    end

    def update_throughput(read_capacity_units, write_capacity_units)
      if @read_capacity_units > read_capacity_units
        @last_decreased_time = Time.now.to_i
      elsif @read_capacity_units < read_capacity_units
        @last_increased_time = Time.now.to_i
      end

      if @write_capacity_units > write_capacity_units
        @last_decreased_time = Time.now.to_i
      elsif @write_capacity_units < write_capacity_units
        @last_increased_time = Time.now.to_i
      end

      @read_capacity_units, @write_capacity_units = read_capacity_units, write_capacity_units
    end
  end
end
