//https://stackoverflow.com/questions/8664705/how-to-read-file-from-end-to-start-in-reverse-order-in-java
import java.io.*;

public class ReverseLineInputStream extends InputStream {
    RandomAccessFile in;
    long currentLineStart = -1;
    long currentLineEnd = -1;
    long currentPos = -1;
    long lastPosInFile = -1;
    public ReverseLineInputStream(File file) throws FileNotFoundException {
        in = new RandomAccessFile(file, "r");
        currentLineStart = file.length();
        currentLineEnd = file.length();
        lastPosInFile = file.length() -1;
        currentPos = currentLineEnd;
    }
    public void findPrevLine() throws IOException {
        currentLineEnd = currentLineStart;
        if (currentLineEnd == 0) {
            currentLineEnd = -1;
            currentLineStart = -1;
            currentPos = -1;
            return;
        }
        long filePointer = currentLineStart -1;
        while ( true) {
            filePointer--;
            if (filePointer < 0) {
                break;
            }
            in.seek(filePointer);
            int readByte = in.readByte();
            if (readByte == 0xA && filePointer != lastPosInFile ) {
                break;
            }
        }
        currentLineStart = filePointer + 1;
        currentPos = currentLineStart;
    }

    public int read() throws IOException {
        if (currentPos < currentLineEnd ) {
            in.seek(currentPos++);
            int readByte = in.readByte();
            return readByte;
        }
        else if (currentPos < 0) {
            return -1;
        }
        else {
            findPrevLine();
            return read();
        }
    }
}
