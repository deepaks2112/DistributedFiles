import java.io.File;
import java.io.FileOutputStream;
import java.util.Scanner;

public class InitializeServerBlocks {
    public static void main(String[] args){
//        char d='A';
        InitializeServerBlocks i=new InitializeServerBlocks();
        Scanner in=new Scanner(System.in);
        int op=in.nextInt();
        if(op==0) {
            i.initialize(1);
            i.initialize(2);
            i.initialize(3);
        }else if(op==1){
            i.read(1);
            i.read(2);
            i.read(3);
        }
    }

    public void initialize(int x){
        String path="/home/deepak/JavaProjects/DFSDraft/src/Servers/S"+x+"/";
        Metadata md;
        for(char c='A';c<='Z';c++){
            md=new Metadata(path+c+".abc");
            md.fetchMetaData();
            if(c=='Z'){
                md.updateMetaData("null",false);
            }else {
                md.updateMetaData(path+(char)(c+1)+".abc", false);
            }
        }// /home/deepak/JavaProjects/DFSDraft/src/Servers/S1/A.abc
        try{
            File fl=new File("/home/deepak/JavaProjects/DFSDraft/src/Servers/S"+x+"/freelist.txt");
            String status=path+"A.abc 26";
            System.out.println(status);
            FileOutputStream fos=new FileOutputStream(fl);
            fos.write(status.getBytes());
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Initialised each block.");
    }

    public void read(int x){
        String path="/home/deepak/JavaProjects/DFSDraft/src/Servers/S"+x+"/";
        Metadata md;
        md=new Metadata(path+"A.abc");
        md.fetchMetaData();
        String next=md.getNextfilename();
        System.out.println(md.getFilename()+"\t"+md.getNextfilename()+"\t"+md.isInuse());
        for(int i=0;i<25;i++){
            md=new Metadata(next);
            md.fetchMetaData();
            next=md.getNextfilename();
            System.out.println(md.getFilename()+"\t"+md.getNextfilename()+"\t"+md.isInuse());
        }
    }
}
