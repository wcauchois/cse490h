package tests;

import java.io.IOException;

import main.AppUtil;
import main.FSUtil;

import edu.washington.cs.cse490h.lib.Node;

import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.Update;

public class ProtobufSerializer extends Node {
    FSUtil fs;
    
    @Override
    public void start() {
        try { 
            fs = new FSUtil(this);
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
    
    @Override
    public void onCommand(String command) {
        if(command.startsWith("serialize")) {
            String[] parsed = AppUtil.splitArgs(command, 2);
            Update update = Update.newBuilder().setType(Update.Type.Write).setVersion(0).setFilename("boobies").setContents("wee").build();
            
            CommitAttempt attempt = CommitAttempt.newBuilder()
                .setId(5)
                .setFrom(-1)
                .addUpdates(update)
                .build();
        
            try {
                fs.writeBinaryContents(parsed[1], attempt.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if(command.startsWith("deserialize")) {
            String[] parsed = AppUtil.splitArgs(command, 2);
            try {
                System.out.println(CommitAttempt.parseFrom(fs.getBinaryContents(parsed[1])));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onReceive(Integer from, int protocol, byte[] msg) {}
}
