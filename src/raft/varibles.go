/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-07-02 10:26:03
 * @LastEditTime: 2021-07-02 13:48:47
 */
package raft

const (
	LEADER         = 1
	FOLLOWER       = 2
	CANDIDATE      = 3
	HEARTBEAT_DEAD = 4
	HEARTBEAT_LIVE = 5
)
