require 'zlib'
module Gelfd2
  class GzipParser

    def self.parse(data)
      begin
        t = Zlib::GzipReader.new(StringIO.new(data))
        decompressed = t.read
        t.close  # may trigger CRCError if there was corruption
        decompressed
      #raise NotYetImplementedError, "GZip decoding is not yet implemented"
      rescue Exception => e
        raise DecodeError, "Failed to decode data: #{e}"
      end
    end

  end
end
