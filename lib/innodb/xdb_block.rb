# -*- encoding : utf-8 -*-
require "innodb/cursor"
require "pp"

# An Xtradb transaction log block.
class Innodb::XdbBlock
  # Log blocks are fixed-length at 4KB in XtraDB.
  BLOCK_SIZE = 4096

  HEADER_SIZE = 28
  HEADER_START = 0

  TRAILER_SIZE = 4
  TRAILER_START = BLOCK_SIZE - TRAILER_SIZE

  RECORD_START = HEADER_START + HEADER_SIZE

#MODIFIED_PAGE_IS_LAST_BLOCK = 0,/* 1 if last block in the current
#				write, 0 otherwise. */
#MODIFIED_PAGE_START_LSN = 4,    /* The starting tracked LSN of this and
#				other blocks in the same write */
#MODIFIED_PAGE_END_LSN = 12, /* The ending tracked LSN of this and
#				other blocks in the same write */
#MODIFIED_PAGE_SPACE_ID = 20,    /* The space ID of tracked pages in
#				this block */
#MODIFIED_PAGE_1ST_PAGE_ID = 24, /* The page ID of the first tracked
#				page in this block */
#MODIFIED_PAGE_BLOCK_UNUSED_1 = 28,/* Unused in order to align the start
#				of bitmap at 8 byte boundary */
#MODIFIED_PAGE_BLOCK_BITMAP = 32,/* Start of the bitmap itself */
#MODIFIED_PAGE_BLOCK_UNUSED_2 = MODIFIED_PAGE_BLOCK_SIZE - 8,
#				/* Unused in order to align the end of
#				bitmap at 8 byte boundary */
#MODIFIED_PAGE_BLOCK_CHECKSUM = MODIFIED_PAGE_BLOCK_SIZE - 4
#				/* The checksum of the current block */

  # Initialize a log block by passing in a 512-byte buffer containing the raw # log block contents.
  def initialize(buffer)
    unless buffer.size == BLOCK_SIZE
      raise "Xdb block buffer provided was not #{BLOCK_SIZE} bytes" 
    end

    @buffer = buffer
  end

  # A helper function to return bytes from the log block buffer based on offset
  # and length, both in bytes.
  def data(offset, length)
    @buffer[offset...(offset + length)]
  end

  # Return an Innodb::Cursor object positioned at a specific offset.
  def cursor(offset)
    Innodb::Cursor.new(self, offset)
  end

  # Return the log block header.
  def header
    @header ||= begin
      c = cursor(HEADER_START)
      {
        :last_block    = > c.get_uint32,
        :start_lsn     = > c.get_uint64,
        :end_lsn       = > c.get_uint64,
        :space_id      = > c.get_uint32,
        :first_page_id = > c.get_uint32,
      }
    end
  end

  # Return the log block trailer.
  def trailer
    @trailer ||= begin
      c = cursor(TRAILER_START)
      {
        :checksum => c.get_uint32,
      }
    end
  end

  # Return the log record. (This is mostly unimplemented.)
  def record
    @record ||= begin
      c = cursor(RECORD_START)
      {
        :bitmap = > c.get_hex,
      }
      end
    end
  end

  # Dump the contents of a log block for debugging purposes.
  def dump
    puts
    puts "header:"
    pp header

    puts
    puts "trailer:"
    pp trailer
  end
end
