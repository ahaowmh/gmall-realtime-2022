package com.atguigu.bean;

/**
 * @author ahao
 * @date 2022/7/23 08:48
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Bean1 {
    private String id;
    private String name;
    private Long ts;
}