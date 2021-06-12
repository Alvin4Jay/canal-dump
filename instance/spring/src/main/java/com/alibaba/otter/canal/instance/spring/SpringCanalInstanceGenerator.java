package com.alibaba.otter.canal.instance.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;

/**
 * @author zebin.xuzb @ 2012-7-12
 * @version 1.0.0
 */
public class SpringCanalInstanceGenerator implements CanalInstanceGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SpringCanalInstanceGenerator.class);
    private String springXml;
    private String defaultName = "instance";
    private BeanFactory beanFactory;

    public CanalInstance generate(String destination) {
        synchronized (CanalInstanceGenerator.class) {
            try {
                // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
                System.setProperty("canal.instance.destination", destination); // 目录
                this.beanFactory = getBeanFactory(springXml);
                // 首先判断beanFactory是否包含以destination为id的bean
                String beanName = destination;
                if (!beanFactory.containsBean(beanName)) {
                    // 如果没有，设置要获取的bean的id为instance
                    beanName = defaultName;
                }

                // 以 destination为id 或者 "instance"为id 获取CanalInstance实例
                return (CanalInstance) beanFactory.getBean(beanName);
            } catch (Throwable e) {
                logger.error("generator instance failed.", e);
                throw new CanalException(e);
            } finally {
                System.setProperty("canal.instance.destination", "");
            }
        }
    }

    private BeanFactory getBeanFactory(String springXml) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(springXml);
        return applicationContext;
    }

    public void setSpringXml(String springXml) {
        this.springXml = springXml;
    }
}
