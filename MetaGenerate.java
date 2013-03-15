import java.io.*;
import org.red5.io.flv.impl.*;
import org.red5.io.*;

public class MetaGenerate {


    public static void main(String args[]) throws Exception {

        File f = new File(args[0]);
        FLVReader flvReader = new FLVReader(f);

        FileKeyFrameMetaCache metaCache = new FileKeyFrameMetaCache();
        metaCache.saveKeyFrameMeta(f, flvReader.analyzeKeyFrames());
        flvReader.close();

        System.exit(0);

    }

}