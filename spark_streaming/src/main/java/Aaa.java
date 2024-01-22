import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName: Aaa
 * @Description: TODO 类描述
 * @Author: Tanjh
 * @Date: 2024/01/18 17:05
 * @Company: Copyright©
 **/

public class Aaa {
    public static void main(String[] args) {
        System.out.println("git使用测试");

        Dog dog = new Dog("dwx",28,"ChengDu","M");

        System.out.println(dog.getName()+"住在"+dog.getHome()+",今年已经"+dog.getAge()+"岁了");
        System.out.println(dog.getSex());
    }




}

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
class Dog{
    private String Name;
    private Integer  age;
    private String Home;
    private String Sex;

}
