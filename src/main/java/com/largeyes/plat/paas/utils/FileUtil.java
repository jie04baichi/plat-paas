package com.largeyes.plat.paas.utils;

import com.largeyes.plat.paas.file.IFileService;

public class FileUtil {
    private static IFileService fileManager; 
    static{
        fileManager=PaasUtilsContextHolder.getBean("fileManager",IFileService.class);
    }
    
    public static String saveFile(String fileName, String fileType)
    {
        return fileManager.saveFile(fileName, fileType);
    }
    public static String saveFile(byte[] byteFile, String fileName, String fileType){
        return fileManager.saveFile(byteFile, fileName, fileType);
    }
    public static byte[] readFile(String fileId){
        return fileManager.readFile(fileId);
    }
    public static void readFile(String fileId, String localFileName){
        fileManager.readFile(fileId, localFileName);
    }
    public static void deleteFile(String fileId){
        fileManager.deleteFile(fileId);
    }
    public static String updateFile(byte[] byteFile, String fileId, String fileName,String fileType) throws Exception{
        return fileManager.updateFile(byteFile, fileId, fileName, fileType);
    }
    public static String getImageUrl(String fileId){
    	return fileManager.getImageUrl(fileId);
    }
    public static String getFilesUrl(String fileId){
    	return fileManager.getFilesUrl(fileId);
    }
}

