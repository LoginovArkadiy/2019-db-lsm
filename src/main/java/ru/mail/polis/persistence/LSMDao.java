package ru.mail.polis.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    private Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int generation;
    private List<FileChannelTable> fileTable1s;

    public LSMDao(final File base, final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        readFiles();
    }

    private void readFiles() throws IOException {
        Stream<Path> stream = Files.walk(base.toPath(), 1).filter(path -> path.getFileName().toString().endsWith(SUFFIX));
        final List<Path> files = stream.collect(Collectors.toList());
        fileTable1s = new ArrayList<>(files.size());
        generation = -1;
        files.forEach(path -> {
            File file = path.toFile();
            try {
                FileChannelTable fileChannelTable = new FileChannelTable(file);
                fileTable1s.add(fileChannelTable);
                generation = Math.max(generation, FileMapTable.getGenerationByName(file.getName()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> list = new ArrayList<>(fileTable1s.size() + 1);
        fileTable1s.forEach(fileChannelTable -> list.add(fileChannelTable.iterator(from)));

        final Iterator<Cell> cells = memTable.iterator(from);
        list.add(cells);

        Iterator<Cell> iterator = Iterators.mergeSorted(list, Cell.COMPARATOR);

        iterator = Iters.collapseEquals(iterator, Cell::getKey);

        final Iterator<Cell> alive =
                Iterators.filter(
                        iterator,
                        cell -> cell == null || cell.getValue() == null || !cell.getValue().isRemoved());

        return Iterators.transform(
                alive,
                cell -> {
                    assert cell != null && cell.getKey() != null;
                    return Record.of(cell.getKey(), cell.getValue().getData());
                });
    }


    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        System.gc();
        memTable.upsert(key.duplicate(), value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
            readFiles();
        }
    }

    private void flush() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)), generation++);
        memTable.clear();
    }

    private void flush(final Iterator<Cell> iterator, int generation) throws IOException {
        final File tmp = new File(base, TABLE_NAME + generation + TEMP);
        FileChannelTable.write(iterator, tmp);
        final File dest = new File(base, TABLE_NAME + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    private void mergeTables(int from, int to) throws IOException {
        List<FileChannelTable> mergeFiles = fileTable1s.subList(from, to);
        fileTable1s = fileTable1s.subList(0, from);
        Iterator<Cell> mergeIterator = FileChannelTable.merge(mergeFiles);
        int generation = -1;
        for (FileChannelTable table : mergeFiles) {
            File file = table.getFile();
            String name = file.getName();
            generation = Math.max(generation, FileMapTable.getGenerationByName(name));
//            Files.delete(file.toPath());
        }

        if (generation >= 0) {
            flush(mergeIterator, generation);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        mergeTables(fileTable1s.size() / 2, fileTable1s.size());
    }
}
