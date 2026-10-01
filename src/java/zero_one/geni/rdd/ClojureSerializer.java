package zero_one.geni.rdd;


import clojure.lang.Util;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;

import scala.reflect.ClassTag;


/**
 * Spark's Java serialisation, as `spark.serializer`, except that it reads
 * booleans back as Boolean.TRUE and Boolean.FALSE (see
 * CanonicalObjectInputStream). Spark uses `spark.serializer` for RDD records,
 * broadcasts and task results, so a false in a Clojure map in an RDD stays
 * false after a shuffle, and when it's collected. DataFrames don't use it.
 *
 * Geni sets it on the local sessions it starts. An executor loads its
 * `spark.serializer` as it starts, before it fetches the application's jars,
 * so on a cluster it only works when Geni's jar is on the executors' own
 * classpath, as with `spark.executor.extraClassPath`.
 *
 * Like Spark's JavaSerializer, it resets its object stream every
 * `spark.serializer.objectStreamReset` objects, 100 by default.
 */
public class ClojureSerializer extends Serializer implements Externalizable {

    private static final long serialVersionUID = 1L;

    private int counterReset;
    private transient volatile ClassLoader loader;


    /**
     * For deserialisation only.
     */
    public ClojureSerializer() {
        this(new SparkConf());
    }


    public ClojureSerializer(SparkConf conf) {
        this.counterReset = conf.getInt("spark.serializer.objectStreamReset", 100);
    }


    @Override
    public Serializer setDefaultClassLoader(ClassLoader classLoader) {
        this.loader = classLoader;
        return super.setDefaultClassLoader(classLoader);
    }


    @Override
    public SerializerInstance newInstance() {
        ClassLoader classLoader = (loader != null)
            ? loader
            : Thread.currentThread().getContextClassLoader();
        return new Instance(counterReset, classLoader);
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(counterReset);
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException {
        counterReset = in.readInt();
    }


    private static final class Instance extends SerializerInstance {

        private final int counterReset;
        private final ClassLoader loader;


        Instance(int counterReset, ClassLoader loader) {
            this.counterReset = counterReset;
            this.loader = loader;
        }


        @Override
        public <T> ByteBuffer serialize(T t, ClassTag<T> tag) {
            Bytes bytes = new Bytes();
            SerializationStream out = serializeStream(bytes);
            out.writeObject(t, tag);
            out.close();
            return bytes.toByteBuffer();
        }


        @Override
        public <T> T deserialize(ByteBuffer bytes, ClassTag<T> tag) {
            return deserializeStream(new ByteBufferInput(bytes), loader).readObject(tag);
        }


        @Override
        public <T> T deserialize(ByteBuffer bytes, ClassLoader classLoader, ClassTag<T> tag) {
            return deserializeStream(new ByteBufferInput(bytes), classLoader).readObject(tag);
        }


        @Override
        public SerializationStream serializeStream(OutputStream s) {
            return new Output(s, counterReset);
        }


        @Override
        public DeserializationStream deserializeStream(InputStream s) {
            return deserializeStream(s, loader);
        }


        private DeserializationStream deserializeStream(InputStream s, ClassLoader classLoader) {
            return new Input(s, classLoader);
        }

    }


    private static final class Output extends SerializationStream {

        private final ObjectOutputStream out;
        private final int counterReset;
        private int counter = 0;


        Output(OutputStream s, int counterReset) {
            try {
                this.out = new ObjectOutputStream(s);
            } catch (IOException ex) {
                throw Util.sneakyThrow(ex);
            }
            this.counterReset = counterReset;
        }


        @Override
        public <T> SerializationStream writeObject(T t, ClassTag<T> tag) {
            try {
                out.writeObject(t);
                counter += 1;
                if (counterReset > 0 && counter >= counterReset) {
                    out.reset();
                    counter = 0;
                }
            } catch (IOException ex) {
                // Spark's callers expect the IOException itself.
                throw Util.sneakyThrow(ex);
            }
            return this;
        }


        @Override
        public void flush() {
            try {
                out.flush();
            } catch (IOException ex) {
                throw Util.sneakyThrow(ex);
            }
        }


        @Override
        public void close() {
            try {
                out.close();
            } catch (IOException ex) {
                throw Util.sneakyThrow(ex);
            }
        }

    }


    private static final class Input extends DeserializationStream {

        private final CanonicalObjectInputStream in;


        Input(InputStream s, ClassLoader classLoader) {
            try {
                this.in = new CanonicalObjectInputStream(s, classLoader);
            } catch (IOException ex) {
                throw Util.sneakyThrow(ex);
            }
        }


        @Override
        @SuppressWarnings("unchecked")
        public <T> T readObject(ClassTag<T> tag) {
            try {
                return (T)in.readObject();
            } catch (IOException | ClassNotFoundException ex) {
                // An EOFException ends asIterator, as with Spark's own.
                throw Util.sneakyThrow(ex);
            }
        }


        @Override
        public void close() {
            try {
                in.close();
            } catch (IOException ex) {
                throw Util.sneakyThrow(ex);
            }
        }

    }


    /**
     * A ByteArrayOutputStream that hands over its buffer without a copy.
     */
    private static final class Bytes extends ByteArrayOutputStream {

        ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }

    }


    /**
     * Reads a ByteBuffer from its position, without moving the buffer's own.
     */
    private static final class ByteBufferInput extends InputStream {

        private final ByteBuffer buffer;


        ByteBufferInput(ByteBuffer buffer) {
            this.buffer = buffer.duplicate();
        }


        @Override
        public int read() {
            return buffer.hasRemaining() ? (buffer.get() & 0xFF) : -1;
        }


        @Override
        public int read(byte[] dest, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (!buffer.hasRemaining()) {
                return -1;
            }
            int n = Math.min(length, buffer.remaining());
            buffer.get(dest, offset, n);
            return n;
        }


        @Override
        public int available() {
            return buffer.remaining();
        }

    }

}
