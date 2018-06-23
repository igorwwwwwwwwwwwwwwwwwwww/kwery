require 'kwery'

RSpec.describe Kwery::Parser::Lexer do
  it "lexes select num" do
    tokens =
    sql = 'SELECT 1'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 1],
    ])
  end

  it "lexes lower case select" do
    tokens =
    sql = 'select 1'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'select'],
      [:NUMBER, 1],
    ])
  end

  it "lexes select num+" do
    tokens =
    sql = 'SELECT 64'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 64],
    ])
  end

  it "lexes select str" do
    tokens =
    sql = "SELECT 'foo'"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, 'foo'],
    ])
  end

  it "lexes select empty str" do
    tokens =
    sql = "SELECT ''"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, ''],
    ])
  end

  it "lexes select escaped str" do
    tokens =
    sql = "SELECT '\\''"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, "'"],
    ])
  end

  it "lexes select bool" do
    tokens =
    sql = 'SELECT true'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:BOOL, true],
    ])
  end

  it "lexes short bool" do
    tokens =
    sql = 'SELECT t'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:BOOL, true],
    ])
  end

  it "lexes select * from users" do
    tokens =
    sql = "SELECT * FROM users"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STAR, '*'],
      [:FROM, 'FROM'],
      [:ID, 'users'],
    ])
  end

  it "lexes select * from users where id = 1" do
    tokens =
    sql = "SELECT * FROM users WHERE id = 1"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STAR, '*'],
      [:FROM, 'FROM'],
      [:ID, 'users'],
      [:WHERE, 'WHERE'],
      [:ID, 'id'],
      [:COMPARE, '='],
      [:NUMBER, 1],
    ])
  end
end
