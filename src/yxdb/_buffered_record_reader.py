from io import BytesIO

from yxdb._lzf import Lzf
from yxdb._utility import memview


class BufferedRecordReader:
    def __init__(self, stream: BytesIO, fixed_len: int, has_var_fields: bool, total_records: int):
        self.lzf_buffer_size = 262144
        self.stream = stream
        self.fixed_len = fixed_len
        self.has_var_fields = has_var_fields
        self.total_records = total_records
        if has_var_fields:
            self.record_buffer = memview(fixed_len + 4 + 1000)
        else:
            self.record_buffer = memview(fixed_len)
        self.lzf_in = memview(self.lzf_buffer_size)
        self.lzf_out = memview(self.lzf_buffer_size)
        self.lzf = Lzf(self.lzf_in, self.lzf_out)
        self.lzf_length_buffer = memview(4)
        self.current_record = 0
        self.record_buffer_index = 0
        self.lzf_out_size = 0
        self.lzf_out_index = 0

    def next_record(self) -> bool:
        self.current_record += 1
        if self.current_record > self.total_records:
            self.stream.close()
            return False
        self.record_buffer_index = 0
        if self.has_var_fields:
            self._read_variable_record()
        else:
            self._read(self.fixed_len)
        return True

    def _read_variable_record(self):
        self._read(self.fixed_len + 4)
        var_length = int.from_bytes(self.record_buffer[self.record_buffer_index-4:self.record_buffer_index], "little")
        if self.fixed_len + 4 + var_length > len(self.record_buffer):
            new_length = (self.fixed_len + 4 + var_length) * 2
            new_buffer = memview(new_length)
            new_buffer[:self.fixed_len+4] = self.record_buffer[:self.fixed_len+4]
            self.record_buffer = new_buffer
        self._read(var_length)

    def _read(self, size: int):
        while size > 0:
            if self.lzf_out_size == 0:
                self.lzf_out_size = self._read_next_lzf_block()

            while size + self.lzf_out_index > self.lzf_out_size:
                size -= self._copy_remaining_lzf_out_to_record()
                self.lzf_out_size = self._read_next_lzf_block()
                self.lzf_out_index = 0

            len_to_copy = min(self.lzf_out_size, size)
            self.record_buffer[self.record_buffer_index:self.record_buffer_index+len_to_copy] = self.lzf_out[self.lzf_out_index: self.lzf_out_index+len_to_copy]
            self.lzf_out_index += len_to_copy
            self.record_buffer_index += len_to_copy
            size -= len_to_copy

    def _copy_remaining_lzf_out_to_record(self) -> int:
        remaining_lzf = self.lzf_out_size - self.lzf_out_index
        self.record_buffer[self.record_buffer_index:self.record_buffer_index+remaining_lzf] = self.lzf_out[self.lzf_out_index:self.lzf_out_index+remaining_lzf]
        self.record_buffer_index += remaining_lzf
        return remaining_lzf

    def _read_next_lzf_block(self) -> int:
        lzf_block_length = self._read_lzf_block_length()
        checkbit = lzf_block_length & 0x80000000
        if checkbit > 0:
            lzf_block_length &= 0x7fffffff
            self.stream.readinto(self.lzf_out[:lzf_block_length])
        else:
            read_in = self.stream.readinto(self.lzf_in[:lzf_block_length])
            return self.lzf.decompress(read_in)

    def _read_lzf_block_length(self):
        read = self.stream.readinto(self.lzf_length_buffer)
        if read < 4:
            raise IOError
        return int.from_bytes(self.lzf_length_buffer, "little")
