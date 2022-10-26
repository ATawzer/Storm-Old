select sr.run_id,
        rh.run_date,
        ti.*,
        case when stm.track_id is not null then 1 else 0 end as target_track
from inferred_storm_run_membership sr
        left join inferred_run_history rh on sr.run_id = rh.run_id
        left join track_info ti on sr.track_id = ti._id
        left join storm_target_membership stm on ti._id = stm.track_id
                                                and stm.storm_name = rh.storm_name
