require 'spec_helper'

module FakeDynamo

  class FilterTest
    include Filter
  end


  describe Filter do

    def encode(bytes)
      Base64.encode64(bytes.pack('c*'))
    end

    subject { FilterTest.new }

    let(:bstr) { encode([1, 2, 3]) }
    let(:s_attr) { Attribute.new('test', 'bcd', 'S')}
    let(:ss_attr) { Attribute.new('test', ['ab', 'cd'], 'SS') }
    let(:n_attr) { Attribute.new('test', '10', 'N')}
    let(:ns_attr) { Attribute.new('test', ['1', '2', '3', '4'], 'NS')}
    let(:b_attr) { Attribute.new('test', bstr, 'B')}
    let(:bs_attr) { Attribute.new('test', [bstr], 'BS')}


    it 'tests eq' do
      subject.eq_filter([{'S' => 'bcd'}], s_attr, false).should be_true
      subject.eq_filter([{'B' => bstr}], b_attr, false).should be_true
      subject.eq_filter([{'S' => '10'}], n_attr, false).should be_false
      expect { subject.eq_filter([{'S' => '10'}], n_attr, true) }.to raise_error(ValidationException, /mismatch/)
    end

    it 'tests le' do
      subject.le_filter([{'S' => 'c'}], s_attr, false).should be_true
      subject.le_filter([{'S' => 'bcd'}], s_attr, false).should be_true
      subject.le_filter([{'S' => 'a'}], s_attr, false).should be_false
      subject.le_filter([{'N' => '10'}], n_attr, false).should be_true
      subject.le_filter([{'N' => '11'}], n_attr, false).should be_true
      subject.le_filter([{'N' => '1'}], n_attr, false).should be_false
      subject.le_filter([{'B' => encode([1, 1])}], b_attr, false).should be_false
      subject.le_filter([{'B' => encode([1, 2, 3])}], b_attr, false).should be_true
    end

    it 'tests lt' do
      subject.lt_filter([{'S' => 'c'}], s_attr, false).should be_true
      subject.lt_filter([{'S' => 'bcd'}], s_attr, false).should be_false
      subject.lt_filter([{'S' => 'a'}], s_attr, false).should be_false
      subject.lt_filter([{'N' => '10'}], n_attr, false).should be_false
      subject.lt_filter([{'N' => '11'}], n_attr, false).should be_true
      subject.lt_filter([{'N' => '1'}], n_attr, false).should be_false
      subject.lt_filter([{'B' => encode([1, 2])}], b_attr, false).should be_false
      subject.lt_filter([{'B' => encode([1, 4])}], b_attr, false).should be_true
      subject.lt_filter([{'B' => encode([1, 2, 3])}], b_attr, false).should be_false
    end

    it 'test ge' do
      subject.ge_filter([{'S' => 'c'}], s_attr, false).should be_false
      subject.ge_filter([{'S' => 'bcd'}], s_attr, false).should be_true
      subject.ge_filter([{'S' => 'a'}], s_attr, false).should be_true
      subject.ge_filter([{'N' => '10'}], n_attr, false).should be_true
      subject.ge_filter([{'N' => '11'}], n_attr, false).should be_false
      subject.ge_filter([{'N' => '1'}], n_attr, false).should be_true
      subject.ge_filter([{'B' => encode([1, 1])}], b_attr, false).should be_true
      subject.ge_filter([{'B' => encode([1, 2, 3])}], b_attr, false).should be_true
    end

    it 'test gt' do
      subject.gt_filter([{'S' => 'c'}], s_attr, false).should be_false
      subject.gt_filter([{'S' => 'bcd'}], s_attr, false).should be_false
      subject.gt_filter([{'S' => 'a'}], s_attr, false).should be_true
      subject.gt_filter([{'N' => '10'}], n_attr, false).should be_false
      subject.gt_filter([{'N' => '11'}], n_attr, false).should be_false
      subject.gt_filter([{'N' => '1'}], n_attr, false).should be_true
      subject.gt_filter([{'B' => encode([1, 1])}], b_attr, false).should be_true
      subject.gt_filter([{'B' => encode([1, 2, 3])}], b_attr, false).should be_false
    end

    it 'test begins_with' do
      subject.begins_with_filter([{'S' => 'bc'}], s_attr, false).should be_true
      subject.begins_with_filter([{'S' => 'cd'}], s_attr, false).should be_false
      subject.begins_with_filter([{'B' => encode([1, 1])}], b_attr, false).should be_false
      subject.begins_with_filter([{'B' => encode([1, 2])}], b_attr, false).should be_true
      expect {
        subject.begins_with_filter([{'N' => '10'}], n_attr, false)
      }.to raise_error(ValidationException, /not supported/)
    end

    it 'test between' do
      expect {
        subject.between_filter([{'S' => 'bc'}], s_attr, false)
      }.to raise_error(ValidationException, /argument count/)
      subject.between_filter([{'S' => 'a'},{'S' => 'c'}], s_attr, false).should be_true
      subject.between_filter([{'S' => 'bcd'},{'S' => 'bcd'}], s_attr, false).should be_true
      subject.between_filter([{'N' => '9'},{'N' => '11'}], n_attr, false).should be_true
      subject.between_filter([{'S' => '9'},{'S' => '11'}], n_attr, false).should be_false
      subject.between_filter([{'B' => encode([1, 1])}, {'B' => encode([1, 2])}], b_attr, false).should be_false
      subject.between_filter([{'B' => encode([1, 2, 2])}, {'B' => encode([1, 2, 4])}], b_attr, false).should be_true
    end

    it 'test ne' do
      subject.ne_filter([{'S' => 'bcd'}], s_attr, false).should be_false
      subject.ne_filter([{'S' => '10'}], n_attr, false).should be_false
      subject.ne_filter([{'S' => 'xx'}], s_attr, false).should be_true
      subject.ne_filter([{'N' => '10.0'}], n_attr, false).should be_false
      subject.ne_filter([{'B' => bstr}], b_attr, false).should be_false
    end

    it 'test not null' do
      subject.not_null_filter(nil, nil, false).should be_false
      subject.not_null_filter(nil, s_attr, false).should be_true
    end

    it 'test  null' do
      subject.null_filter(nil, nil, false).should be_true
      subject.null_filter(nil, s_attr, false).should be_false
    end

    it 'test contains' do
      subject.contains_filter([{'S' => 'cd'}], s_attr, false).should be_true
      subject.contains_filter([{'S' => 'cd'}], ss_attr, false).should be_true
      subject.contains_filter([{'N' => '2'}], ns_attr, false).should be_true
      subject.contains_filter([{'N' => '10'}], n_attr, false).should be_false
      subject.contains_filter([{'B' => encode([1])}], b_attr, false).should be_true
    end

    it 'test not contains' do
      subject.not_contains_filter([{'S' => 'xx'}], s_attr, false).should be_true
      subject.not_contains_filter([{'S' => 'cd'}], s_attr, false).should be_false
      subject.not_contains_filter([{'S' => 'cd'}], ss_attr, false).should be_false
      subject.not_contains_filter([{'N' => '2'}], ns_attr, false).should be_false
      subject.not_contains_filter([{'N' => '12'}], ns_attr, false).should be_true
      subject.not_contains_filter([{'N' => '10'}], n_attr, false).should be_false
      subject.not_contains_filter([{'B' => encode([1])}], b_attr, false).should be_false
    end

    it 'test in' do
      subject.in_filter([{'S' => 'bcd'}], s_attr, false).should be_true
      subject.in_filter([{'S' => 'bcd'}, {'N' => '10'}], n_attr, true).should be_true
      subject.in_filter([{'N' => '1'}], ns_attr, true).should be_false
      subject.in_filter([{'S' => 'xx'}], s_attr, false).should be_false
      subject.in_filter([{'B' => encode([1, 2, 3])}], b_attr, false).should be_true
    end
  end
end
