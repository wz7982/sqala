package bench.jvm;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface BenchUserMapper {

    @Select("SELECT id, name, email, age, active, score, created_at, bio, city, balance " +
            "FROM bench_user WHERE id = #{id}")
    BenchUser findById(@Param("id") long id);

    @Select("SELECT id, name, email, age, active, score, created_at, bio, city, balance " +
            "FROM bench_user WHERE id >= #{start} AND id < #{end}")
    List<BenchUser> findRange(@Param("start") long start, @Param("end") long end);
}
