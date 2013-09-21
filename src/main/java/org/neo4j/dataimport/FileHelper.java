package org.neo4j.dataimport;

import java.io.*;

public class FileHelper {
    public static void writeToFile(InputStream uploadedInputStream,
                                   String uploadedFileLocation)
    {
        OutputStream out = null;
        File f = new File( uploadedFileLocation );
        try
        {

            out = new FileOutputStream( f );
            int read = 0;
            byte[] bytes = new byte[1024];

            out = new FileOutputStream( new File( uploadedFileLocation ) );
            while ( (read = uploadedInputStream.read( bytes )) != -1 )
            {
                out.write( bytes, 0, read );
            }
            out.flush();
        }
        catch ( IOException e )
        {

            e.printStackTrace();
        }
        finally
        {
            if ( out != null )
            {
                try
                {
                    uploadedInputStream.close();
                    out.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }

    }
}
