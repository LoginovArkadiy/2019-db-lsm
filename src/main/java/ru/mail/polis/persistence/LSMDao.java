package ru.mail.polis.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public class LSMDao implements DAO {
    private static final String TABLE_NAME = "SSTable";
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final int DANGER_COUNT_FILES = 5;
    private final Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int currentGeneration;
    private List<Table> fileTables;

    /**
     * NoSql Dao.
     *
     * @param base           directory of DB
     * @param flushThreshold maxsize of @memTable
     * @throws IOException If an I/O error occurs
     */
    public LSMDao(final File base, final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        readFiles();
    }

    private void readFiles() throws IOException {
        try (Stream<Path> stream = Files.walk(base.toPath(), 1)
                .filter(path -> {
                    final String fileName = path.getFileName().toString();
                    return fileName.endsWith(SUFFIX) && fileName.contains(TABLE_NAME);
                })) {
            final List<Path> files = stream.collect(Collectors.toList());
            fileTables = new ArrayList<>(files.size());
            currentGeneration = -1;
            files.forEach(path -> {
                final File file = path.toFile();
                try {
                    final FileChannelTable fileChannelTable = new FileChannelTable(file);
                    fileTables.add(fileChannelTable);
                    currentGeneration = Math.max(currentGeneration,
                            FileChannelTable.getGenerationByName(file.getName()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            currentGeneration++;
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> list = new ArrayList<>(fileTables.size() + 1);
        for (final Table fileChannelTable : fileTables) {
            list.add(fileChannelTable.iterator(from));
        }
        final Iterator<Cell> memoryIterator = memTable.iterator(from);
        list.add(memoryIterator);
        final Iterator<Cell> iterator = Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR));

        final Iterator<Cell> alive =
                Iterators.filter(
                        iterator,
                        cell -> !cell.getValue().isRemoved());

        return Iterators.transform(
                alive,
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private void updateData() throws IOException {
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
            if (fileTables.size() > DANGER_COUNT_FILES) {
                mergeTables(0, fileTables.size() / 3);
            }
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
        updateData();
    }

    private void flush() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)), currentGeneration++, memTable.getBloomFilter());
        memTable.clear();
    }

    private void flush(final Iterator<Cell> iterator,
            final int generation,
            final BitSet bloomFilter)
            throws IOException {
        final File tmp = new File(base, generation + TABLE_NAME + TEMP);
        Table.write(iterator, tmp, bloomFilter);
        final File dest = new File(base, generation + TABLE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        fileTables.add(new FileChannelTable(dest));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        updateData();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        final Cell memCell = memTable.get(key);
        if (memCell != null) {
            if (memCell.getValue().isRemoved()) {
                throw new NoSuchElementException("");
            }
            return memCell.getValue().getData();
        }

        final List<Cell> cells = new ArrayList<>();
        fileTables.forEach(table -> {
            try {
                final Cell cell = table.get(key);
                if (cell != null) {
                    cells.add(cell);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


        if (cells.isEmpty()) {
            throw new NoSuchElementException("");
        }

        final Cell cell = Collections.min(cells, Cell.COMPARATOR);
        if (cell == null || cell.getValue().isRemoved()) {
            throw new NoSuchElementException("");
        }
        final Record record = Record.of(cell.getKey(), cell.getValue().getData());
        return record.getValue();
    }

    private void mergeTables(final int from, final int to) throws IOException {
        final List<Table> mergeFiles = fileTables.subList(from, to);
        final Iterator<Cell> mergeIterator = FileChannelTable.merge(mergeFiles);
        int generation = -1;
        final BitSet mergeBloomFilter = new BitSet();
        for (final Table table : mergeFiles) {
            if (table instanceof FileChannelTable) {
                final FileChannelTable fileTable = (FileChannelTable) table;
                final File file = fileTable.getFile();
                final String name = file.getName();
                generation = Math.max(generation, FileChannelTable.getGenerationByName(name));
            }
            mergeBloomFilter.or(table.getBloomFilter());
        }

        fileTables = fileTables.subList(0, from);
        if (generation >= 0) {
            flush(mergeIterator, generation, mergeBloomFilter);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
