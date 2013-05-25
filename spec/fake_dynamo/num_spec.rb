require 'spec_helper'

module FakeDynamo
  describe Num do
    it 'should validate number' do
      ['10000000000000000000000000000000000101',
        '0.1',
        '1000000000000000000000.0000000000000101',
        '.00000000000000000000000000000000000001011',
        '.10000000000000000000000000000000001011',
        '.1e126',
        '-.1e126',
        '-3'].each do |n|
        Num.new(n)
      end
    end

    it 'should raise on number larger that 38 significant digits' do
      ['1000000000000000000000.00000000000001011',
        '1.00000000000000000000000000000000001011',
        '.100000000000000000000000000000000001011'].each do |n|
        expect { Num.new(n) }.to raise_error(ValidationException, /significant/)
      end
    end

    it 'shoud raise on overflow' do
      ['.1e127',
        '-.1e127'].each do |n|
        expect { Num.new(n) }.to raise_error(ValidationException, /Number .*flow/)
      end
    end
  end
end
