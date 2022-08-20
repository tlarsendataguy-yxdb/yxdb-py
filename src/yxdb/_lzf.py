class Lzf:
    def __init__(self, in_bytes: memoryview, out_bytes: memoryview):
        self.in_bytes = in_bytes
        self.out_bytes = out_bytes
        self.iidx = 0
        self.oidx = 0
        self.in_len = 0

    def decompress(self, length: int) -> int:
        self.in_len = length
        self.reset()

        if self.in_len == 0:
            return 0

        while self.iidx < self.in_len:
            ctrl: int = self.in_bytes[self.iidx]
            self.iidx += 1

            if ctrl < 32:
                self._copy_byte_sequence(ctrl)
            else:
                self._expand_repeated_bytes(ctrl)

        return self.oidx

    def reset(self):
        self.iidx = 0
        self.oidx = 0

    def _copy_byte_sequence(self, ctrl: int):
        length: int = ctrl + 1
        if self.oidx + length > len(self.out_bytes):
            raise AttributeError

        self.out_bytes[self.oidx:self.oidx + length] = self.in_bytes[self.iidx: self.iidx + length]
        self.oidx += length
        self.iidx += length

    def _expand_repeated_bytes(self, ctrl: int):
        length: int = ctrl >> 5
        reference: int = self.oidx - ((ctrl & 0x1f) << 8) - 1  # magic

        if length == 7:
            length += self.in_bytes[self.iidx]
            self.iidx += 1

        if self.oidx + length + 2 > len(self.out_bytes):
            raise AttributeError

        reference -= self.in_bytes[self.iidx]
        self.iidx += 1

        length += 2

        while length > 0:
            size = min(self.oidx - reference, length)
            reference = self._copy_from_reference_and_increment(reference, size)
            length -= size

    def _copy_from_reference_and_increment(self, reference: int, size: int) -> int:
        self.out_bytes[self.oidx:self.oidx+size] = self.out_bytes[reference:reference+size]
        self.oidx += size
        return reference + size
