require 'kwery'

RSpec.describe Kwery::Parser::Lexer do
  it "parses select num" do
    sql = 'SELECT 1'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, 1]]
    )
  end

  it "parses lower case select" do
    sql = 'select 1'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, 1]]
    )
  end

  it "parses select num+" do
    sql = 'SELECT 64'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, 64]]
    )
  end

  it "parses select str" do
    sql = "SELECT 'foo'"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, 'foo']]
    )
  end

  it "parses select empty str" do
    sql = "SELECT ''"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, '']]
    )
  end

  it "parses select escaped str" do
    sql = "SELECT '\\''"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, "'"]]
    )
  end

  it "parses select bool" do
    sql = 'SELECT true'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, true]]
    )
  end

  it "parses short bool" do
    sql = 'SELECT t'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:value, true]]
    )
  end

  it "parses select * from users" do
    sql = "SELECT * FROM users"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:id, '*'],
       [:from, 'users']]
    )
  end

  it "parses select * from users where id = 1" do
    sql = "SELECT * FROM users WHERE id = 1"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:id, '*'],
       [:from, 'users'],
       [:where, ['=', [:id, 'id'], [:value, 1]]]]
    )
  end

  it "parses select name from users where id = 1" do
    sql = "SELECT name FROM users WHERE id = 1"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      [:select, [:id, 'name'],
       [:from, 'users'],
       [:where, ['=', [:id, 'id'], [:value, 1]]]]
    )
  end
end
