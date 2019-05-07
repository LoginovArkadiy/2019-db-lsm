package ru.mail.polis.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.Iters;

public class FileChannelTable implements Table {
    private static final String UNSUPPORTED_EXCEPTION_MESSAGE = "FileTable has not access to update!";
    private final int rows;
    private final File file;

    /**
     * Sorted String Table, which use FileChannel for Read_and_Write operations.
     *
     * @param file of this table
     * @throws IOException If an I/O error occurs
     */
    public FileChannelTable(final File file) throws IOException {
        this.file = file;
        try (FileChannel fc = openReadFileChannel()) {
            // Rows
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            assert fc != null;
            fc.read(buffer, fc.size() - Long.BYTES);

            final long rowsValue = buffer.rewind().getLong();
            assert rowsValue <= Integer.MAX_VALUE;
            this.rows = (int) rowsValue;
        }
    }

    /**
     * Dump to the file in directory.
     * List of Cells
     * -Cell
     * keySize - int.
     * key - ByteBuffer sizeOf(key) = keySize.
     * Timestamp - time of last update.
     * if timestamp is positive then next
     * valueSize - int
     * value - ByteBuffer sizeOf(value) = valueSize.
     * -offsets of each row.
     * -count rows
     *
     * @param cells iterator of data
     * @param to    directory
     * @throws IOException If an I/O error occurs
     */
    static void write(final Iterator<Cell> cells, final File to) throws IOException {
        try (FileChannel fc = FileChannel.open(to.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final Cell cell = cells.next();
                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();

                final Value value = cell.getValue();

                final int bufferSize = Integer.BYTES
                        + key.remaining()
                        + Long.BYTES
                        + (value.isRemoved() ? 0 : Integer.BYTES + value.getData().remaining());

                final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                buffer.putInt(keySize).put(key);

                // Timestamp
                if (value.isRemoved()) {
                    buffer.putLong(-cell.getValue().getTimeStamp());
                } else {
                    buffer.putLong(cell.getValue().getTimeStamp());
                }

                // Value
                if (!value.isRemoved()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = valueData.remaining();
                    buffer.putInt(valueSize).put(valueData);
                }

                buffer.flip();
                fc.write(buffer);
                offset += bufferSize;
            }

            // Offsets
            for (final long anOffset : offsets) {
                fc.write(Bytes.fromLong(anOffset));
            }

            // Rows
            fc.write(Bytes.fromLong(offsets.size()));
        }
    }

    /**
     * Merge list of SSTables.
     *
     * @param tables list of SSTables
     * @return MergedIterator with latest versions of key-value
     */
    public static Iterator<Cell> merge(final List<Table> tables) {
        if (tables == null || tables.isEmpty()) {
            return new ArrayList<Cell>().iterator();
        }
        final List<Iterator<Cell>> list = new ArrayList<>(tables.size());
        for (final Table table : tables) {
            try {
                list.add(table.iterator(ByteBuffer.allocate(0)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR));
    }

    public File getFile() {
        return file;
    }

    private FileChannel openReadFileChannel() {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int readInt(final FileChannel fc, final long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private long readLong(final FileChannel fc, final long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getLong();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1L;
    }

    private ByteBuffer readBuffer(final FileChannel fc, final long offset, final int size) {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            fc.read(buffer, offset);
            return buffer.rewind();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    private long getOffset(final FileChannel fc, final int i) {
        final ByteBuffer offsetBB = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(offsetBB, fc.size()
                    - Long.BYTES
                    - Long.BYTES * rows
                    + Long.BYTES * i);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return offsetBB.rewind().getLong();
    }

    @NotNull
    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;
            long offset = getOffset(fc, i);
            assert offset <= Integer.MAX_VALUE;

            // KeySize
            final int keySize = readInt(fc, offset);
            offset += Integer.BYTES;

            // Key
            return readBuffer(fc, offset, keySize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;
            long offset = getOffset(fc, i);
            assert offset <= Integer.MAX_VALUE;

            //KeySize
            final int keySize = readInt(fc, offset);
            offset += Integer.BYTES;

            //Key
            final ByteBuffer key = readBuffer(fc, offset, keySize);
            offset += keySize;

            //Timestamp
            final long timeStamp = readLong(fc, offset);
            offset += Long.BYTES;

            if (timeStamp < 0) {
                return new Cell(key, Value.tombstone(-timeStamp));
            }
            //valueSize
            final int valueSize = readInt(fc, offset);
            offset += Integer.BYTES;

            //value
            final ByteBuffer value = readBuffer(fc, offset, valueSize);
            return new Cell(key, Value.of(timeStamp, value));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + ((right - left) >> 1);
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

    static int getGenerationByName(final String name) {
        for (int index = 0; index < Math.min(9, name.length()); index++) {
            if (!Character.isDigit(name.charAt(index))) {
                return index == 0 ? 0 : Integer.parseInt(name.substring(0, index));
            }
        }
        return -1;
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
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator is empty");
                }
                return cellAt(next++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException(UNSUPPORTED_EXCEPTION_MESSAGE);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException(UNSUPPORTED_EXCEPTION_MESSAGE);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(UNSUPPORTED_EXCEPTION_MESSAGE);
    }
}
