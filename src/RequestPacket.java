import java.io.Serializable;

public class RequestPacket implements Serializable {

    private String command;
    private String secondaryCommand;
    private String filename;
    private int numOfBlocks;
    private int indexOfBlock;
    private byte[][] data;
    private MDS mds;
    private int[] offset;
    private int blocksToDelete;


    public RequestPacket(String command,String filename,int numOfBlocks,byte[][] data){
        this.command=command;
        this.filename=filename;
        this.numOfBlocks=numOfBlocks;
        this.data=data;
        this.mds=new MDS("","","","");
        this.secondaryCommand="";
        this.offset=new int[1];
        this.offset[0]=-1;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public void setSecondaryCommand(String secondaryCommand) {
        this.secondaryCommand = secondaryCommand;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setNumOfBlocks(int numOfBlocks) {
        this.numOfBlocks = numOfBlocks;
    }

    public void setIndexOfBlock(int indexOfBlock) {
        this.indexOfBlock = indexOfBlock;
    }

    public void setData(byte[][] data) {
        this.data = data;
    }

    public void setMds(MDS mds) {
        this.mds = mds;
    }

    public void setOffset(int[] offset) {
        this.offset = offset;
    }

    public void setBlocksToDelete(int blocksToDelete) {
        this.blocksToDelete = blocksToDelete;
    }

    public RequestPacket(String command, String filename, int numOfBlocks, byte[][] data, MDS mds){
        this.command=command;
        this.filename=filename;
        this.numOfBlocks=numOfBlocks;
        this.data=data;
        this.mds=mds;
        this.secondaryCommand="";
        this.offset=new int[1];
        this.offset[0]=-1;
        this.blocksToDelete=0;
    }

    public RequestPacket(String command,String filename,int numOfBlocks,byte[][] data,MDS mds,String secondaryCommand){
        new RequestPacket(command,filename,numOfBlocks,data,mds);
        this.secondaryCommand=secondaryCommand;
        this.offset=new int[1];
        this.offset[0]=-1;
    }

    public RequestPacket(String command,String filename,int numOfBlocks,byte[][] data,MDS mds,String secondaryCommand,int[] offset){
        new RequestPacket(command,filename,numOfBlocks,data,mds,secondaryCommand);
        this.offset=offset;
    }

    public RequestPacket(String command,String filename,int numOfBlocks,byte[][] data,MDS mds,String secondaryCommand,int[] offset,int blocksToDelete){
        new RequestPacket(command,filename,numOfBlocks,data,mds,secondaryCommand,offset);
        this.blocksToDelete=blocksToDelete;
    }

    public int getBlocksToDelete() {
        return blocksToDelete;
    }

    public int[] getOffset() {
        return offset;
    }

    public int getIndexOfBlock() {
        return indexOfBlock;
    }

    public String getSecondaryCommand() {
        return secondaryCommand;
    }

    public MDS getMds() {
        return mds;
    }

    public byte[][] getData() {
        return data;
    }

    public String getCommand() {
        return command;
    }

    public int getNumOfBlocks() {
        return numOfBlocks;
    }

    public String getFilename() {
        return filename;
    }

}