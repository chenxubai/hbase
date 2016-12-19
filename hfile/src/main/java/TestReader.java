import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.FSReader;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class TestReader {  

    public static String FILE_PATH = "H:\\testdata\\hbase\\hfile\\f130f0dbff1f4c2e9681ce574b38edbd";  
    public Configuration cfg = new Configuration();  
    private FSReader fsBlockReader;  
    /** 
     * 二级索引长度 
     */  
    private static final int SECONDARY_INDEX_ENTRY_OVERHEAD = Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;  

    public static void main(String[] args) throws Exception {  
        TestReader t = new TestReader();  
        t.readScan();
    }  

    /** 
     * 解析布隆过滤器 
     */  
    public void readBloom() throws IOException {  
        // 创建读取路径，本地文件系统，两个读取流  
        Path path = new Path(FILE_PATH);  
        FileSystem fs = FileSystem.getLocal(cfg);  
        CacheConfig config = new CacheConfig(cfg);  

        // 由HFile创建出Reader实现类  
        Reader reader = HFile.createReader(fs, path, config,cfg);  

        // 创建通用布隆过滤器  
        DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();  
        BloomFilter bloomFilter = null;  
        if (bloomMeta != null) {  
            bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);  
            System.out.println(bloomFilter);  
        }  

        //创建删除的布隆过滤器  
        bloomMeta = reader.getDeleteBloomFilterMetadata();  
        bloomFilter = null;  
        if (bloomMeta != null) {  
            bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);  
            System.out.println(bloomFilter);  
        }  
          
        //meta的读取实现在  HFileReaderV2#getMetaBlock()中  
    }  

    /** 
     * 使用Scanner读取数据块内容 
     */  
    @SuppressWarnings("unchecked")  
    public void readScan() throws IOException, SecurityException,  
            NoSuchMethodException, IllegalArgumentException,  
            IllegalAccessException, InvocationTargetException {  
        // 创建读取路径，本地文件系统，两个读取流  
        Path path = new Path(FILE_PATH);  
        FileSystem fs = FileSystem.getLocal(cfg);  
        CacheConfig config = new CacheConfig(cfg);  
        FSDataInputStream fsdis = fs.open(path);  
        FSDataInputStreamWrapper fsdisw = new FSDataInputStreamWrapper(fsdis);
        FSDataInputStream fsdisNoFsChecksum = fsdis;  
        HFileSystem hfs = new HFileSystem(fs);  
        long size = fs.getFileStatus(path).getLen();  

        // 由读FS读取流，文件长度，就可以读取到尾文件块  
        FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, size);  
        
        HFileReaderV3 v3 = new HFileReaderV3(path, trailer, fsdisw, size, config, hfs, cfg);
        
        // 根据尾文件块，和其他相关信息，创建HFile.Reader实现  
/*        HFileReaderV2 v2 = new HFileReaderV2(path, trailer, fsdis,  
                fsdisNoFsChecksum, size, true, config, DataBlockEncoding.NONE,  
                hfs);  */
        System.out.println(v3);  

        // 读取FileInfo中的内容  
        Method method = v3.getClass().getMethod("loadFileInfo", new Class[] {});  
        Map<byte[], byte[]> fileInfo = (Map<byte[], byte[]>) method.invoke(v3,  
                new Object[] {});  
        Iterator<Entry<byte[], byte[]>> iter = fileInfo.entrySet().iterator();  
/*        while (iter.hasNext()) {  
            Entry<byte[], byte[]> entry = iter.next();  
            System.out.println(Bytes.toString(entry.getKey()) + " = "  
                    + Bytes.toShort(entry.getValue()));  
        } */ 

        // 由Reader实现创建扫描器Scanner，负责读取数据块  
        // 并遍历所有的数据块中的KeyValue  
        HFileScanner scanner = v3.getScanner(false, false);  
        scanner.seekTo();  
        System.out.println(scanner.getKeyValue());  

        KeyValue kv = scanner.getKeyValue();  
        while (scanner.next()) {  
            kv = scanner.getKeyValue();  
            System.out.println(kv);  
        }  
        v3.close();  

    }  

    /** 
     * 解析HFile中的数据索引 
     */  
    @SuppressWarnings({ "unused", "unchecked" })  
    public void readIndex() throws Exception {/*  
        // 创建读取路径，本地文件系统，两个读取流  
        // 由读FS读取流，文件长度，就可以读取到尾文件块  
        Path path = new Path(FILE_PATH);  
        FileSystem fs = FileSystem.getLocal(cfg);  
        CacheConfig config = new CacheConfig(cfg);  
        FSDataInputStream fsdis = fs.open(path);  
        FSDataInputStream fsdisNoFsChecksum = fsdis;  
        HFileSystem hfs = new HFileSystem(fs);  
        long size = fs.getFileStatus(path).getLen();  
        FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, size);  

        // 下面创建的一些类，在Reader实现类的构造函数中也可以找到，创建具体文件读取实现FSReader  
        // 由于这个类没有提供对外的创建方式，只能通过反射构造 FSReader  
        Compression.Algorithm compressAlgo = trailer.getCompressionCodec();  
        Class<?> clazz = Class  
                .forName("org.apache.hadoop.hbase.io.hfile.HFileBlock$FSReaderV2");  
        java.lang.reflect.Constructor<FSReader> constructor = (Constructor<FSReader>) clazz  
                .getConstructor(new Class[] { FSDataInputStream.class,  
                        FSDataInputStream.class, Compression.Algorithm.class,  
                        long.class, int.class, HFileSystem.class, Path.class });  
        constructor.setAccessible(true);  
        fsBlockReader = constructor.newInstance(fsdis, fsdis, compressAlgo,  
                size, 0, hfs, path);  

        // 创建比较器，比较器是定义在尾文件块中  
        RawComparator<byte[]> comparator = FixedFileTrailer  
                .createComparator(KeyComparator.class.getName());  

        // 创建读取数据块的根索引  
        BlockIndexReader dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(  
                comparator, trailer.getNumDataIndexLevels());  

        // 创建读取元数据快的根索引  
        BlockIndexReader metaBlockIndexReader = new HFileBlockIndex.BlockIndexReader(  
                Bytes.BYTES_RAWCOMPARATOR, 1);  

        // 创建 HFileBlock 迭代器  
        HFileBlock.BlockIterator blockIter = fsBlockReader.blockRange(  
                trailer.getLoadOnOpenDataOffset(),  
                size - trailer.getTrailerSize());  

        // 读取数据文件根索引  
        dataBlockIndexReader.readMultiLevelIndexRoot(  
                blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX),  
                trailer.getDataIndexCount());  

        // 读取元数据根索引  
        metaBlockIndexReader.readRootIndex(  
                blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX),  
                trailer.getMetaIndexCount());  

        // 读取FileInfo块中的信息  
        // 由于FileInfo块不是public的，所以定义了一个MyFileInfo，内容跟FileInfo一样  
        long fileinfoOffset = trailer.getFileInfoOffset();  
        HFileBlock fileinfoBlock = fsBlockReader.readBlockData(fileinfoOffset,  
                -1, -1, false);  
        MyFileInfo fileinfo = new MyFileInfo();  
        fileinfo.readFields(fileinfoBlock.getByteStream());  
        int avgKeyLength = Bytes.toInt(fileinfo.get(MyFileInfo.AVG_KEY_LEN));  
        int avgValueLength = Bytes  
                .toInt(fileinfo.get(MyFileInfo.AVG_VALUE_LEN));  
        long entryCount = trailer.getEntryCount();  
        System.out.println("avg key length=" + avgKeyLength);  
        System.out.println("avg value length=" + avgValueLength);  
        System.out.println("entry count=" + entryCount);  

        int numDataIndexLevels = trailer.getNumDataIndexLevels();  
        if (numDataIndexLevels > 1) {  
            // 大于一层  
            iteratorRootIndex(dataBlockIndexReader);  
        } else {  
            // 单根索引  
            iteratorSingleIndex(dataBlockIndexReader);  
        }  

        fsdis.close();  
        fsdisNoFsChecksum.close();  
    */}  

    /** 
     * 解析单层索引 
     */  
    public void iteratorSingleIndex(BlockIndexReader dataBlockIndex) {  
        for (int i = 0; i < dataBlockIndex.getRootBlockCount(); i++) {  
            byte[] keyCell = dataBlockIndex.getRootBlockKey(i);  
            int blockDataSize = dataBlockIndex.getRootBlockDataSize(i);  
            String rowKey = parseKeyCellRowkey(keyCell);  
            System.out.println("rowkey=" + rowKey + "\tdata size="  
                    + blockDataSize);  
        }  
    }  

    /** 
     * 解析多层索引，首先解析根索引 
     */  
    public void iteratorRootIndex(BlockIndexReader dataBlockIndex)  
            throws IOException {  
        for (int i = 0; i < dataBlockIndex.getRootBlockCount(); i++) {  
            long offset = dataBlockIndex.getRootBlockOffset(i);  
            int onDiskSize = dataBlockIndex.getRootBlockDataSize(i);  
            iteratorNonRootIndex(offset, onDiskSize);  
        }  
    }  

    /** 
     * 递归解析每个中间索引 
     */  
    public void iteratorNonRootIndex(long offset, int onDiskSize)  
            throws IOException {  
        HFileBlock block = fsBlockReader.readBlockData(offset, onDiskSize, -1,  
                false);  
        if (block.getBlockType().equals(BlockType.LEAF_INDEX)) {  
            parseLeafIndex(block);  
            return;  
        }  
        // 开始计算中间层索引的 每个key位置  
        ByteBuffer buffer = block.getBufferReadOnly();  

        buffer = ByteBuffer.wrap(buffer.array(),  
                buffer.arrayOffset() + block.headerSize(),  
                buffer.limit() - block.headerSize()).slice();  
        int indexCount = buffer.getInt();  

        // 二级索引全部偏移量，二级索引数据+二级索引总数(int)+索引文件总大小(int)  
        int entriesOffset = Bytes.SIZEOF_INT * (indexCount + 2);  
        for (int i = 0; i < indexCount; i++) {  
            // 二级索引指向的偏移量  
            // 如当前遍历到第一个key，那么二级索引偏移量就是 第二个int(第一个是索引总数)  
            int indexKeyOffset = buffer.getInt(Bytes.SIZEOF_INT * (i + 1));  
            long blockOffsetIndex = buffer.getLong(indexKeyOffset  
                    + entriesOffset);  
            int blockSizeIndex = buffer.getInt(indexKeyOffset + entriesOffset  
                    + Bytes.SIZEOF_LONG);  
            iteratorNonRootIndex(blockOffsetIndex, blockSizeIndex);  
        }  
    }  

    /** 
     * 解析叶索引 
     */  
    public void parseLeafIndex(HFileBlock block) {  
        // 开始计算中间层索引的 每个key位置  
        ByteBuffer buffer = block.getBufferReadOnly();  
        buffer = ByteBuffer.wrap(buffer.array(),  
                buffer.arrayOffset() + block.headerSize(),  
                buffer.limit() - block.headerSize()).slice();  
        int indexCount = buffer.getInt();  

        // 二级索引全部偏移量，二级索引数据+二级索引总数(int)+索引文件总大小(int)  
        int entriesOffset = Bytes.SIZEOF_INT * (indexCount + 2);  
        for (int i = 0; i < indexCount; i++) {  
            // 二级索引指向的偏移量  
            // 如当前遍历到第一个key，那么二级索引偏移量就是 第二个int(第一个是索引总数)  
            int indexKeyOffset = buffer.getInt(Bytes.SIZEOF_INT * (i + 1));  

            // 全部二级索引长度+key偏移位置+ 块索引offset(long)+块大小(int)  
            // 可以计算出真实的key的偏移位置  
            int KeyOffset = entriesOffset + indexKeyOffset  
                    + SECONDARY_INDEX_ENTRY_OVERHEAD;  
            // long blockOffsetIndex =  
            // buffer.getLong(indexKeyOffset+entriesOffset);  
            int blockSizeIndex = buffer.getInt(indexKeyOffset + entriesOffset  
                    + Bytes.SIZEOF_LONG);  

            // 计算key的长度  
            int length = buffer.getInt(Bytes.SIZEOF_INT * (i + 2))  
                    - indexKeyOffset - SECONDARY_INDEX_ENTRY_OVERHEAD;  

            // 一个key  
            // cell包含了key长度(2字节),key,family长度(1字节),family,qualifier,timestampe(8字节),keytype(1字节)  
            // 这里只需要key就可以了  
            byte[] keyCell = new byte[length];  
            System.arraycopy(buffer.array(), buffer.arrayOffset() + KeyOffset,  
                    keyCell, 0, length);  

            String rowKey = parseKeyCellRowkey(keyCell);  
            System.out.println("rowkey=" + rowKey + "\t blockSizeIndex="  
                    + blockSizeIndex);  
        }  
    }  

    /** 
     * 通过keycell，解析出rowkey 
     */  
    public static String parseKeyCellRowkey(byte[] cell) {  
        if (cell == null || cell.length < 3) {  
            throw new IllegalArgumentException("cell length is illegal");  
        }  
        int high = (cell[0] >> 8) & 0xFF;  
        int low = cell[1] & 0xFF;  
        int keySize = high + low;  
        byte[] key = new byte[keySize];  
        System.arraycopy(cell, 2, key, 0, key.length);  
        return Bytes.toString(key);  
    }  

}  