# -*- encoding : utf-8 -*-

require "innodb/data_type"

# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records, including the length
# of the fixed-width and variable-width portion of the field.
class Innodb::Field
  attr_reader :position, :name, :data_type, :nullable

  # Size of a reference to data stored externally to the page.
  EXTERN_FIELD_SIZE = 20

  def initialize(position, name, type_definition, *properties)
    @position = position
    @name = name
    @nullable = properties.delete(:NOT_NULL) ? false : true
    base_type, modifiers = parse_type_definition(type_definition.to_s)
    @data_type = Innodb::DataType.new(base_type, modifiers, properties)
  end

  # Return whether this field can be NULL.
  def nullable?
    @nullable
  end

  # Return whether this field is NULL.
  def null?(record)
    nullable? && record[:header][:field_nulls][position]
  end

  # Return whether a part of this field is stored externally (off-page).
  def extern?(record)
    record[:header][:field_externs][position]
  end

  def variable?
    @data_type.is_a? Innodb::DataType::BlobType or
    @data_type.is_a? Innodb::DataType::VariableBinaryType or
    @data_type.is_a? Innodb::DataType::VariableCharacterType or
    @data_type.is_a? Innodb::DataType::VariableIntegerType
  end

  def blob?
    @data_type.is_a? Innodb::DataType::BlobType
  end

  # Return the actual length of this variable-length field.
  def length(record)
    if variable?
      len = record[:header][:field_lengths][position]
    else
      len = @data_type.width
    end
    extern?(record) ? len - EXTERN_FIELD_SIZE : len
  end

  # Read an InnoDB encoded data field.
  def read(record, cursor)
    cursor.name(@data_type.name) { cursor.get_bytes(length(record)) }
  end

  # Read the data value (e.g. encoded in the data).
  def value(record, cursor)
    return :NULL if null?(record)
    data = read(record, cursor)
    @data_type.respond_to?(:value) ? @data_type.value(data) : data
  end

  # Read an InnoDB external pointer field.
  def extern(record, cursor)
    return nil if not extern?(record)
    cursor.name(@name) { read_extern(cursor) }
  end

  private

  # Return an external reference field. An extern field contains the page
  # address and the length of the externally stored part of the record data.
  def get_extern_reference(cursor)
    {
      :space_id     => cursor.name("space_id")    { cursor.get_uint32 },
      :page_number  => cursor.name("page_number") { cursor.get_uint32 },
      :offset       => cursor.name("offset")      { cursor.get_uint32 },
      :length       => cursor.name("length")      { cursor.get_uint64 & 0x3fffffff }
    }
  end

  def read_extern(cursor)
    cursor.name("extern") { get_extern_reference(cursor) }
  end

  # Parse a data type definition and extract the base type and any modifiers.
  def parse_type_definition(type_string)
    if matches = /^([a-zA-Z0-9]+)(\(([0-9, ]+)\))?$/.match(type_string)
      base_type = matches[1].upcase.to_sym
      if matches[3]
        modifiers = matches[3].sub(/[ ]/, "").split(/,/).map { |s| s.to_i }
      else
        modifiers = []
      end
      [base_type, modifiers]
    end
  end
end
