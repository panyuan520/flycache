保存数据结构规划：
	key:value 保存结构
		key		value
		
		
	list 保存结构
		list:0  value
		list:1  value
		list:2  value
		list:3  value
		list:4  value
		
	map 保存结构
		map:key1  value
		map:key2  value
		map:key3  value
		map:key4  value
		map:key5  value
		
	zset 没想好是用list数据结构还是用 key:value数据结构来保存，待确认。



请求数据结构规划：
	get  获取
	
	save 保存
	
	delte 删除
	
	filter 筛选排序，暂未想好更优化的快速查询方法，待定


多重数据结构：在value值那里写指向的key



客户端请看：https://github.com/panyuan520/flyclient
	


1:0
1:7
2:1
3:2
3:3
3:6
4:4
5:5
8:8