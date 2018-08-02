package com.largeyes.plat.paas.file;

public interface IFileService {
    public String saveFile(String fileName, String fileType);
    public String saveFile(byte[] byteFile, String fileName, String fileType);
    public byte[] readFile(String fileId);
    public void readFile(String fileId, String localFileName);
    public void deleteFile(String fileId);
    public String updateFile(byte[] byteFile, String fileId, String fileName,String fileType) throws Exception;
    public String getImageUrl(String fileId);
    public String getFilesUrl(String fileId);
}

