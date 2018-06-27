require 'kwery'

RSpec.describe Kwery::Parser::Lexer do
  it "lexes select num" do
    sql = 'SELECT 1'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 1],
    ])
  end

  it "lexes lower case select" do
    sql = 'select 1'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'select'],
      [:NUMBER, 1],
    ])
  end

  it "lexes select num+" do
    sql = 'SELECT 64'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 64],
    ])
  end

  it "lexes select str" do
    sql = "SELECT 'foo'"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, 'foo'],
    ])
  end

  it "lexes select empty str" do
    sql = "SELECT ''"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, ''],
    ])
  end

  it "lexes select escaped str" do
    sql = "SELECT '\\''"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, "'"],
    ])
  end

  it "lexes select bool" do
    sql = 'SELECT true'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:BOOL, true],
    ])
  end

  it "lexes short bool" do
    sql = 'SELECT t'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:BOOL, true],
    ])
  end

  it "lexes select name from users" do
    sql = "SELECT name FROM users"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:ID, :name],
      [:FROM, 'FROM'],
      [:ID, :users],
    ])
  end

  it "lexes select name from users where id = 1" do
    sql = "SELECT name FROM users WHERE id = 1"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:ID, :name],
      [:FROM, 'FROM'],
      [:ID, :users],
      [:WHERE, 'WHERE'],
      [:ID, :id],
      [:EQ, '='],
      [:NUMBER, 1],
    ])
  end
end
