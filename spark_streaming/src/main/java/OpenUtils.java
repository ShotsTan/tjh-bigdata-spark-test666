

import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.Mode;
import cn.hutool.crypto.Padding;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.RSA;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * 开放平台工具类
 * <p>
 * 需要添加依赖：
 * <dependency>
 *   <groupId>cn.hutool</groupId>
 *   <artifactId>hutool-all</artifactId>
 *   <version>5.8.12</version>
 * </dependency>
 * <dependency>
 *   <groupId>com.alibaba</groupId>
 *   <artifactId>fastjson</artifactId>
 *   <version>1.2.83</version>
 * </dependency>
 * </p>
 */
@Slf4j
public class OpenUtils {

    /**
     * 生成RSA公私钥
     */
    public static JSONObject genRSAKey() {
        JSONObject resJson = new JSONObject();
        RSA rsa = new RSA();
        String publicKey = rsa.getPublicKeyBase64();
        String privateKey = rsa.getPrivateKeyBase64();
        resJson.put("publicKey", publicKey);
        resJson.put("privateKey", privateKey);
        log.info("生成的RSA公私钥信息: {}", resJson.toJSONString());
        return resJson;
    }

    /**
     * 使用公钥进行RSA加密
     * @param data      加密体
     * @param publicKey RSA公钥
     * @return Base64格式的字符串
     */
    public static String RSAEncrypt2Base64(String data, String publicKey) {
        RSA rsa = new RSA(null, publicKey);
        return rsa.encryptBase64(data, KeyType.PublicKey);
    }

    /**
     * 使用私钥进行RSA解密
     * @param data       解密体
     * @param privateKey RSA私钥
     * @return 解密后的字符串
     */
    public static String RSADecrypt2Str(String data, String privateKey) {
        RSA rsa = new RSA(privateKey, null);
        return rsa.decryptStr(data, KeyType.PrivateKey);
    }

    /**
     * AES加密：AES/CBC/PKCS5Padding
     * 偏移量：E08ADE2699714B87
     * @param data   加密体
     * @param aesKey aes秘钥，支持三种密钥长度：128、192、256位
     * @return Hex格式字符串
     */
    public static String AESEncrypt2Hex(String data, String aesKey) {
        AES aes = new AES(Mode.CBC, Padding.PKCS5Padding, aesKey.getBytes(), "E08ADE2699714B87".getBytes());
        String encryptData = aes.encryptHex(data);
        return encryptData;
    }

    /**
     * AES解密：AES/CBC/PKCS5Padding
     * 偏移量：E08ADE2699714B87
     * @param data   Hex（16进制）或Base64表示的字符串
     * @param aesKey aes秘钥，支持三种密钥长度：128、192、256位
     * @return UTF-8编码字符串
     */
    public static String AESDecrypt(String data, String aesKey) {
        AES aes = new AES(Mode.CBC, Padding.PKCS5Padding, aesKey.getBytes(), "E08ADE2699714B87".getBytes());
        String decryptData = aes.decryptStr(data);
        return decryptData;
    }

    /**
     * 开放接口调用示例
     */
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            // 初始化应用参数
            String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC4/s/rxctQahpOlTHcK8Amss9Mm0nxh5v7NefoLsGfAcfMSwNZUjoUZzsZLpBVGz8v32IFgvnypqubklR4KYLFUMSPZY8CAhnVB4x3sQR3Iu3X2c0KBP1CGq3pO/2BJmdx4/mE+U+6CmbXsansocA2GfnBlDRCJPwC+m9PWuAQAQIDAQAB";
            String reqUrl = "https://tfsmy.chengdu.gov.cn/community-e-housekeeper/openApi/infoSharePlatform/getOpenWillAdvicePage"; // 接口请求地址
//            String reqUrl = "http://localhost:8086/community-e-housekeeper/openApi/infoSharePlatform/getOpenEvaluateExaminationPage";
            String code = "510107"; // 区域编码
            String secretKey = "RzZUVfNIkQtDAWxj"; // 加密秘钥
            // 组装请求参数并加密
            JSONObject bodyJson = new JSONObject(true);
            bodyJson.put("areaCode", "510112115240");
            bodyJson.put("year", "2023");
            bodyJson.put("requestId", "asdaadax3s14sd42");
            // secretKey加密数据
            String data = AESEncrypt2Hex(bodyJson.toJSONString(), secretKey);
            // 公钥加密secretKey
            String key = RSAEncrypt2Base64(secretKey, publicKey);
            JSONObject reqJson = new JSONObject(true);
            reqJson.put("code", code);
            reqJson.put("data", data);
            reqJson.put("key", key);
            log.info("远程调用接口参数: {}", reqJson.toJSONString());
            // 调用接口
            String resStr = HttpUtil.post(reqUrl, reqJson.toJSONString());

//            System.out.println(resStr);
            log.info("远程调用接口返回: {}", resStr);
            JSONObject resJson = JSON.parseObject(resStr);
            if (!Objects.equals(resJson.getIntValue("code"), 200)) {
                System.out.println("报错！！！");
                log.info("远程调用接口失败，错误信息: {}", resJson.getString("msg"));
            }
            // 调用成功，使用secretKey解密
            String resDataStr = resJson.getString("result");

            if (StrUtil.isNotEmpty(resDataStr)) {
                String decryptResDataStr = AESDecrypt(resDataStr, secretKey);
                log.info("解密结果: \n{}", decryptResDataStr);

                System.out.println(decryptResDataStr);
            }
        } catch (Exception e) {
            System.out.println("Warn");
            log.error("执行出错", e);
        } finally {
//            System.out.println("Finnally");
            long end = System.currentTimeMillis();
            log.info("执行时长: {} ms", end - start);
//            System.out.println(11);
        }
    }
}
