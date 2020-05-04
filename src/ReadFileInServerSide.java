import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;

public class ReadFileInServerSide {
    public static void main(String[] args){
        String path="/home/deepak/JavaProjects/DFSDraft/src/Servers/S1/A.abc";
        Metadata md=new Metadata(path);
        md.fetchMetaData();
        md.readFromFile();
        String next=md.getNextfilename();
        byte[][] data=new byte[25][64*1024];
        data[0]=md.getContent();
        for(int i=0;i<24;i++){
            md=new Metadata(next);
            md.fetchMetaData();
            md.readFromFile();
            data[i+1]=md.getContent();
            next=md.getNextfilename();
        }
        try{
            File fw= new File("./reconstructed/a.pdf");
            FileOutputStream fos=new FileOutputStream(fw);
            for(int i=0;i<25;i++){
                fos.write(data[i]);
            }
//            int len=0;
//            for(len=0;len<64*1024-2;len++){
//                if(data[24][len]==(byte)255&&data[24][len+1]==(byte)255&&data[24][len+2]==(byte)255){
//                    break;
//                }
//            }
//            System.out.println("Length of last block: "+len);
//            byte[] lastBlock=new byte[len];
//            for(int i=0;i<len;i++){
//                lastBlock[i]=data[24][i];
//            }
//            fos.write(lastBlock);
            fos.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
