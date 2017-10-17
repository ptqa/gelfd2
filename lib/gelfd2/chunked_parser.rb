module Gelfd2
  class ChunkedParser
    @@chunk_map = Hash.new {|hash,key| hash[key] = {:total_chunks => 0, :last_chunk_time => 0, :chunks => {} } }
    @@msg_timeout = 20
    @@msg_counter = 0

    attr_accessor :message_id, :max_chunks, :decoded_data, :chunks, :seen

    def self.parse(data)
      msg_id = self.parse_chunk(data)
      if @@chunk_map[msg_id][:chunks].size == @@chunk_map[msg_id][:total_chunks]
        @@msg_counter = @@msg_counter+1
        if @@msg_counter == 1000
          @@msg_counter = 0
          self.delete_old_chunks()
        end 
        assemble_chunks(msg_id)
      end
    end

    def self.assemble_chunks(msg_id)
      buff = ''
      chunks = @@chunk_map[msg_id][:chunks]
      chunks.keys.sort.each do |k|
        buff += chunks[k]
      end
      begin
        # TODO
        # This has a chance for an DoS
        # you can send a chunked message as a chunked message
        t = Parser.parse(buff.clone)
        @@chunk_map.delete(msg_id)
        t
      rescue Exception => e
        "Exception: #{e.message}"
      end
    end

    private
    def self.parse_chunk(data)
      header = data[0..1]
      raise NotChunkedDataError, "This doesn't look like a Chunked GELF message!" if header != CHUNKED_MAGIC
      begin
        msg_id = data[2..9].unpack('C*').join
        seq_number, total_number = data[10].ord, data[11].ord
        zlib_chunk = data[12..-1]
        raise TooManyChunksError, "#{total_number} greater than #{MAX_CHUNKS}" if total_number > MAX_CHUNKS
        @@chunk_map[msg_id][:total_chunks] = total_number.to_i
        @@chunk_map[msg_id][:chunks].merge!({seq_number.to_i => zlib_chunk})
        @@chunk_map[msg_id][:last_chunk_time] = Time.now.getutc.to_i
        msg_id
      end
    end

    private
    def self.delete_old_chunks()
      time = Time.now.getutc.to_i
      begin
        @@chunk_map.each do |msg_id, msg|
          if msg[:last_chunk_time]+@@msg_timeout > time
            next
          end
          @@chunk_map.delete(msg_id)
        end
      end
    end

  end
end
