package com.atguigu.bean;

/**
 * @author ahao
 * @date 2022/7/28 21:43
 */
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 加购独立用户数
    Long cartAddUuCt;

    // 时间戳
    Long ts;
}
