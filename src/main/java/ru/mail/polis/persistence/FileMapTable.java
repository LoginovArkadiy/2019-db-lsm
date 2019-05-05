package ru.mail.polis.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import org.jetbrains.annotations.NotNull;

public class FileMapTable implements Table {
    private final int rows;
    private final LongBuffer offsets;
    private final ByteBuffer cells;
    private final File file;


    FileMapTable(final File file) throws IOException {
        this.file = file;
        final long fileSize = file.length();
//        final ByteBuffer mapped = ByteBuffer.allocate((int) fileSize);
        final ByteBuffer mapped;
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
//            int size = fc.read(mapped);
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
            int limit = mapped.limit();
            // Rows
            final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES));
            assert rowsValue <= Integer.MAX_VALUE;
            this.rows = (int) rowsValue;

            // Offset
            final ByteBuffer offsetBuffer = mapped.duplicate();
            offsetBuffer.position(limit - Long.BYTES * rows - Long.BYTES);
            offsetBuffer.limit(limit - Long.BYTES);
            this.offsets = offsetBuffer.slice().asLongBuffer();

            // Cells
            final ByteBuffer cellBuffer = mapped.duplicate();
            cellBuffer.position(0);
            cellBuffer.limit(offsetBuffer.position());
            this.cells = cellBuffer.slice();
        }
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;
        final int keySize = cells.getInt((int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = cells.duplicate();
        key.position((int) offset);
        offset += keySize;
        key.limit((int) offset);
        return key.slice();
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        //Key
        final int keySize = cells.getInt((int) offset);
        offset += Integer.BYTES;
        ByteBuffer key = cells.duplicate();
        key = key.position((int) offset).limit((int) (offset + keySize)).slice();
        offset += keySize;

        //Timestamp
        final long timeStamp = cells.getLong((int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return new Cell(key, Value.tombstone(-timeStamp));
        } else {
            final int valueSize = cells.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = cells.duplicate();
            value.position((int) offset);
            offset += valueSize;
            value.limit((int) offset);
            return new Cell(key, Value.of(timeStamp, value.slice()));
        }
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @Override
    public long sizeInBytes() {
        return 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    public File getFile() {
        return file;
    }

    static int getGenerationByName(final String name) {
        int index = name.lastIndexOf(".");
        String s = name.substring(0, index);
        index = s.length();
        while (index > 0 && Character.isDigit(s.charAt(index - 1))) {
            index--;
        }
        return Integer.parseInt(s.substring(index));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("");
    }

}
