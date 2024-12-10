package com.tianji.learning.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.tianji.common.utils.BeanUtils;
import com.tianji.common.utils.CollUtils;
import com.tianji.common.utils.DateUtils;
import com.tianji.common.utils.UserContext;
import com.tianji.learning.constants.RedisConstants;
import com.tianji.learning.domain.po.PointsRecord;
import com.tianji.learning.domain.vo.PointsStatisticsVO;
import com.tianji.learning.enums.PointsRecordType;
import com.tianji.learning.mapper.PointsRecordMapper;
import com.tianji.learning.service.IPointsRecordService;
import lombok.RequiredArgsConstructor;
import org.bouncycastle.operator.AsymmetricKeyUnwrapper;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 学习积分记录，每个月底清零 服务实现类
 * </p>
 *
 * @author 虎哥
 */
@Service
@RequiredArgsConstructor
public class PointsRecordServiceImpl extends ServiceImpl<PointsRecordMapper, PointsRecord> implements IPointsRecordService {

    private final StringRedisTemplate redisTemplate;
//    private final IPointsRecordService pointsRecordService;

    @Override
    public void addPointsRecord(Long userId, int points, PointsRecordType type) {
        LocalDateTime now = LocalDateTime.now();
        int maxPoints = type.getMaxPoints();
        // 1.判断当前方式有没有积分上限
        int realPoints = points;
        if(maxPoints > 0) {
            // 2.有，则需要判断是否超过上限
            LocalDateTime begin = DateUtils.getDayStartTime(now);
            LocalDateTime end = DateUtils.getDayEndTime(now);
            // 2.1.查询今日已得积分
            int currentPoints = queryUserPointsByTypeAndDate(userId, type, begin, end);
            // 2.2.判断是否超过上限
            if(currentPoints >= maxPoints) {
                // 2.3.超过，直接结束
                return;
            }
            // 2.4.没超过，保存积分记录
            if(currentPoints + points > maxPoints){
                realPoints = maxPoints - currentPoints;
            }
        }
        // 3.没有，直接保存积分记录
        PointsRecord p = new PointsRecord();
        p.setPoints(realPoints);
        p.setUserId(userId);
        p.setType(type);
        save(p);
        // 4.更新总积分到Redis
        String key = RedisConstants.POINTS_BOARD_KEY_PREFIX + now.format(DateUtils.POINTS_BOARD_SUFFIX_FORMATTER);
        redisTemplate.opsForZSet().incrementScore(key, userId.toString(), realPoints);
    }

    @Override
    public List<PointsStatisticsVO> queryMyPointsToday() {
//        1. 用户id
        Long userId = UserContext.getUser();
//        2. 构建查询语句: 签到,课程学习,课程评论...
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime begin = DateUtils.getDayStartTime(now);
        LocalDateTime end = DateUtils.getDayEndTime(now) ;

        QueryWrapper<PointsRecord> wrapper = new QueryWrapper<>();
//        利用PointsRecord中的points属性暂存"当日已获得积分"
        wrapper.select("type,sum(points) as points");
        wrapper.eq("user_id", userId);
        wrapper.between("create_time",begin,end);
        wrapper.groupBy("type");
        List<PointsRecord> list = this.list(wrapper);
        if (CollUtils.isEmpty(list)){
            return CollUtils.emptyList();
        }
//        3. 封装结果并返回
        List<PointsStatisticsVO> res = new ArrayList<>();
        for (PointsRecord r : list){
            PointsStatisticsVO vo = new PointsStatisticsVO();
            vo.setMaxPoints(r.getType().getMaxPoints());
            vo.setPoints(r.getPoints());
            vo.setType(r.getType().getDesc()); //getDesc()获取枚举类的String类型
            res.add(vo);
        }
        return res;
    }

    private int queryUserPointsByTypeAndDate(
            Long userId, PointsRecordType type, LocalDateTime begin, LocalDateTime end) {
        // 1.查询条件
//        QueryWrapper<PointsRecord> wrapper = new QueryWrapper<>();
//        wrapper.lambda()
//                .eq(PointsRecord::getUserId, userId)
//                .eq(type != null, PointsRecord::getType, type)
//                .between(begin != null && end != null, PointsRecord::getCreateTime, begin, end);
//        // 2.调用mapper，查询结果
//        Integer points = getBaseMapper().queryUserPointsByTypeAndDate(wrapper);
//        // 3.判断并返回
//        return points == null ? 0 : points;
        // 1. 构建查询语句
        QueryWrapper<PointsRecord> wrapper = new QueryWrapper<>();
        wrapper.select("sum(points) as totalPoints FROM PointsRecord");
        wrapper.eq("user_id", userId)
                .eq("type", type)
                .between("create_time",begin,end);
        // 2. 获得查询结果
        Map<String, Object> map = this.getMap(wrapper);

        // 3. 返回查询结果
        if (map != null){
            BigDecimal points = (BigDecimal) map.get("totalPoints");
            return points.intValue();
        }
        // 返回值为-1,代表失败.
        return -1;
    }
}
