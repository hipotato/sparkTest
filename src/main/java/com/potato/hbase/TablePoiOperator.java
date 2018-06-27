package com.potato.hbase;

import com.iwhere.hbase_util.utils.HBasePropertiesTool;
import com.iwhere.hbase_util.utils.HBaseRowkeyUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * poi表操作类（分中英文库）
 * 
 * @author potato
 *
 */
public class TablePoiOperator {
	private final static String iwhereGoTableZh = HBasePropertiesTool.getProperties().getProperty("poiIwhereGoZh");
	private final static String iwhereGoTableEn = HBasePropertiesTool.getProperties().getProperty("poiIwhereGoEn");
	private final static byte[] baseFamily = Bytes.toBytes("base");
	private final static byte[] extraFamily = Bytes.toBytes("extra");
	private final static int maxResults = 3000;


	/**
	 * GeoSOT根据经纬度获取多层网格码信息(分中英文库)
	 * @param lang：语言，中文：0， 英文：1
	 * @param geoNum 网格码
	 * @param geoLevel 网格层级
	 * @param types 类型筛选
	 * @param isAll 是否包含深度信息，true:包含，false：不包含
	 * @return
	 */
	public JSONArray getGridPoiLang(final int lang, final long geoNum, final int geoLevel, final List<String> types, final boolean isAll) {
		// 调用GeoSOT接口获取
		ExecutorService executor = Executors.newSingleThreadExecutor();
		List<Result> results = null;
		try {
			results = getDataLang(lang, geoNum, geoLevel, types);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		int count = 0;
		JSONObject data = new JSONObject();
		JSONArray poiArray = new JSONArray();
		JSONObject baseObj = new JSONObject();
		JSONObject extraObj = new JSONObject();
		for (Result result : results) {
			JSONObject baseObject = new JSONObject();
			JSONObject extraObject = new JSONObject();
			JSONObject poiRoot = new JSONObject();
			count++;
			Map<byte[], byte[]> baseMap = new HashMap<byte[], byte[]>();

			// base列族
			baseMap = result.getFamilyMap(baseFamily);
			byte[] rowKeyByte = result.getRow();

			for (Map.Entry<byte[], byte[]> entry : baseMap.entrySet()) {
				String key = Bytes.toString(entry.getKey());
				String value = Bytes.toString(entry.getValue());
				baseObject.put(key, value);
			}
			poiRoot.put("base", baseObject);
			// 如果需要全量数据，则处理extra列族
			if (isAll) {
				Map<byte[], byte[]> extraMap = new HashMap<byte[], byte[]>();
				extraMap = result.getFamilyMap(extraFamily);
				for (Map.Entry<byte[], byte[]> entry : extraMap.entrySet()) {
					String key = new String((byte[]) entry.getKey());
					String value = new String((byte[]) entry.getValue());
					extraObject.put(key, value);
				}
				poiRoot.put("extra", extraObject);
			}
			poiArray.add(poiRoot);
			// 如果结果数超过maxResults 则break
			if (count > maxResults)
				break;
		}
		return poiArray;
	}

	/**
	 * 获取单个Poi
	 * @param lang 语言，中文：0， 英文：1
	 * @param geoNum 网格码
	 * @param geoLevel 网格层级
	 * @param ids poiId列表
	 * @param isAll 是否包含深度信息，true:包含，false：不包含
	 * @return
	 */
	public JSONArray getPois(final int lang, final long geoNum, final int geoLevel, final List<String> ids, final boolean isAll) {
		// 调用GeoSOT接口获取
		ExecutorService executor = Executors.newSingleThreadExecutor();
		JSONArray reJson = new JSONArray();
		List<Result> results =  new ArrayList<Result>();

		Table table = null;
		try {
			if (lang == 0) {
				table = HBaseConManager.getInstance().getHTableInstanceByName(0, iwhereGoTableZh);
			} else {
				table = HBaseConManager.getInstance().getHTableInstanceByName(0, iwhereGoTableEn);
			}
			long a = -1L;
			byte[] param = Bytes.toBytes(geoNum);
			long c = a >>> 2 * geoLevel;
			long stopRow = geoNum | c;
			Scan scan = new Scan();
			scan.setStartRow(param);
			scan.setStopRow(Bytes.toBytes(stopRow));
			FuzzyRowFilter rowFilter = null;

			// 使用行健模糊筛选器
			if (!(ids == null || ids.isEmpty())) {
				List<Pair<byte[], byte[]>> list = new ArrayList<Pair<byte[], byte[]>>();
				ArrayList<Filter> listForFilters = new ArrayList<Filter>();
				for (String id : ids) {
					SingleColumnValueFilter filter = new SingleColumnValueFilter(baseFamily, Bytes.toBytes("id"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(id));
					listForFilters.add(filter);
				}
				Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,
						listForFilters);
				scan.setFilter(filterList);
			}
			ResultScanner rs = table.getScanner(scan);
			if (rs == null) {
				return reJson;
			}
			for (Result result : rs) {
				JSONObject baseObject = new JSONObject();
				JSONObject extraObject = new JSONObject();
				JSONObject poiRoot = new JSONObject();
				Map<byte[], byte[]> baseMap = new HashMap<byte[], byte[]>();

				// base列族
				baseMap = result.getFamilyMap(baseFamily);
				byte[] rowKeyByte = result.getRow();

				for (Map.Entry<byte[], byte[]> entry : baseMap.entrySet()) {
					String key = Bytes.toString(entry.getKey());
					String value = Bytes.toString(entry.getValue());
					baseObject.put(key, value);
				}
				poiRoot.put("base", baseObject);
				// 如果需要全量数据，则处理extra列族
				if (isAll) {
					Map<byte[], byte[]> extraMap = new HashMap<byte[], byte[]>();
					extraMap = result.getFamilyMap(extraFamily);
					for (Map.Entry<byte[], byte[]> entry : extraMap.entrySet()) {
						String key = new String((byte[]) entry.getKey());
						String value = new String((byte[]) entry.getValue());
						extraObject.put(key, value);
					}
					poiRoot.put("extra", extraObject);
				}
				reJson.add(poiRoot);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return reJson;
	}

	// 获取iwhereGo的poi数据(根据语言)
	private List<Result> getDataLang(int lang, long geoNum, int geoLevel, List<String> types) throws IOException {
		Table table = null;
		if (lang == 0) {
			table = HBaseConManager.getInstance().getHTableInstanceByName(0, iwhereGoTableZh);
		} else {
			table = HBaseConManager.getInstance().getHTableInstanceByName(0, iwhereGoTableEn);
		}
		List<Result> results = new ArrayList<Result>();
		long a = -1L;

		byte[] param = Bytes.toBytes(geoNum);

		long c = a >>> 2 * geoLevel;
		long stopRow = geoNum | c;
		Scan scan = new Scan();
		scan.setStartRow(param);
		scan.setStopRow(Bytes.toBytes(stopRow));
		FuzzyRowFilter rowFilter = null;
		// 使用行健模糊筛选器
		if (!(types == null || types.isEmpty())) {
			List<Pair<byte[], byte[]>> list = new ArrayList<Pair<byte[], byte[]>>();
			for (String type : types) {
				Pair<byte[], byte[]> pair = null;
				byte[] rowKey = HBaseRowkeyUtil.getRowkeyIwhereId(String.valueOf(geoNum), geoLevel, 1, type, (short) 0);
				if (type.length() == 2) {
					pair = new Pair<byte[], byte[]>(rowKey, new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1 });
				} else if (type.length() == 4) {
					pair = new Pair<byte[], byte[]>(rowKey, new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1 });
				} else if (type.length() == 6) {
					pair = new Pair<byte[], byte[]>(rowKey, new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1 });
				} else {
					return results;
				}
				// byte[] rowKey =
				// HBaseRowkeyUtil.getNewRowkey(String.valueOf(geoNum),
				// geoLevel, type, "00000000000");

				// 网格码（8byte）+网格层级（1byte）+楼层（1byte）+poi类型（3byte）+PoiID（10byte）
				list.add(pair);
			}
			rowFilter = new FuzzyRowFilter(list);
			scan.setFilter(rowFilter);
		}
		ResultScanner rs = table.getScanner(scan);
		if (rs == null) {
			return null;
		}
		for (Result result : rs) {
			results.add(result);
		}
		table.close();
		return results;
	}


	/**
	 * 批量更新poi数据（中文库）
	 * 
	 * @param jsonArray
	 *            数据体
	 * @return
	 */
	public JSONObject updateBatchZh(JSONArray jsonArray) {

		ExecutorService executor = Executors.newSingleThreadExecutor();
		JSONArray result=null;
		Future<JSONObject> future = executor.submit(new Callable<JSONObject>(){

			@Override
			public JSONObject call() throws Exception {
				// TODO Auto-generated method stub
				
				return null;
			}
			
		});
		
		JSONObject re = new JSONObject();
		Table table = null;
		try {;
			table = HBaseConManager.getInstance().getHTableInstanceByName(1, iwhereGoTableZh);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		List<String> addedPoi = new ArrayList<>();
		List<String> deletedPoi = new ArrayList<>();
		List<String> errorPoi = new ArrayList<>();
		List<Delete> deletSet = new ArrayList<>();
		List<Put> putsSet = new ArrayList<>();
		for (int i = 0; i < jsonArray.size(); ++i) {
			JSONObject object = (JSONObject) jsonArray.get(i);

			if (!(object.has("geo_num") & object.has("geo_level") & object.has("flag") & object.has("typecode")
					& object.has("id") & object.has("iwhere_index"))) {
				if (object.has("id")) {
					errorPoi.add(object.getString("id"));
				} else {
					errorPoi.add("unkown");
				}
				continue;
			}
			String geoNum = object.getString("geo_num");
			String level = object.getString("geo_level");
			int iLevel = Integer.parseInt(level);
			String flag = object.getString("flag");
			String typecode = object.getString("typecode");
			String id = object.getString("id");
			String index = object.getString("iwhere_index");
			int iFloor = 1;
			if (object.has("floor")) {
				iFloor = object.getInt("floor");
			}
			short sIndex = Short.parseShort(index);
			byte[] rowkey = null;
			try {
				rowkey = HBaseRowkeyUtil.getRowkeyIwhereId(geoNum, iLevel, iFloor, typecode, sIndex);
			} catch (Exception e) {
				// TODO: handle exception
				errorPoi.add(object.getString("id"));
				continue;
			}
			// 如果flag!=0，只标注删除，并不实际删除
//			if (!flag.equals("0")) {
//				Delete delete = new Delete(rowkey);
//				deletSet.add(delete);
//				deletedPoi.add(id);
//				continue;
//			}
			Put put = new Put(rowkey);
			Timestamp time = new Timestamp(System.currentTimeMillis());
			put.addColumn(baseFamily, Bytes.toBytes("geoNum"), Bytes.toBytes(geoNum));
			put.addColumn(baseFamily, Bytes.toBytes("level"), Bytes.toBytes(level));
			put.addColumn(baseFamily, Bytes.toBytes("flag"), Bytes.toBytes(flag));
			put.addColumn(baseFamily, Bytes.toBytes("typecode"), Bytes.toBytes(typecode));
			put.addColumn(baseFamily, Bytes.toBytes("id"), Bytes.toBytes(id));
			put.addColumn(baseFamily, Bytes.toBytes("update_time"), Bytes.toBytes(time.toString()));
			// 处理base字段
			if (object.has("base")) {
				JSONObject base = object.getJSONObject("base");
				Iterator iterator = base.keys();
				while (iterator.hasNext()) {
					String key = (String) iterator.next();
					String value = base.getString(key);
					put.addColumn(baseFamily, Bytes.toBytes(key), Bytes.toBytes(value));
				}
			}
			// 处理extra字段
			if (object.has("extra")) {
				JSONObject extra = object.getJSONObject("extra");
				Iterator iterator = extra.keys();
				{
					while (iterator.hasNext()) {
						String key = (String) iterator.next();
						put.addColumn(extraFamily, Bytes.toBytes(key), Bytes.toBytes(extra.getString(key)));
					}
				}
			}
			putsSet.add(put);
			addedPoi.add(id);
		}
		try {
			if (!putsSet.isEmpty()) {
				table.put(putsSet);
			}
			if (!deletedPoi.isEmpty()) {
				table.delete(deletSet);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();

			return null;
		}
		re.put("addNum", addedPoi.size());
		re.put("addList", addedPoi.toString());
		re.put("deleteNum", deletedPoi.size());
		re.put("deleteList", deletedPoi.toString());
		re.put("errorNum", errorPoi.size());
		re.put("errorList", errorPoi.toString());

		return re;
	}

	/**
	 * 批量更新poi数据（英文库）
	 * 
	 * @param jsonArray
	 *            数据体
	 * @return
	 */
	public JSONObject updateBatchEn(JSONArray jsonArray) {

		JSONObject reMap = new JSONObject();
		Table table = null;
		try {;
			table = HBaseConManager.getInstance().getHTableInstanceByName(1, iwhereGoTableEn);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		List<String> addedPoi = new ArrayList<>();
		List<String> deletedPoi = new ArrayList<>();
		List<String> errorPoi = new ArrayList<>();
		List<Delete> deletSet = new ArrayList<>();
		List<Put> putsSet = new ArrayList<>();
		for (int i = 0; i < jsonArray.size(); ++i) {
			JSONObject object = (JSONObject) jsonArray.get(i);

			if (!(object.has("geo_num") & object.has("geo_level") & object.has("flag") & object.has("typecode")
					& object.has("id") & object.has("iwhere_index"))) {
				if (object.has("id")) {
					errorPoi.add(object.getString("id"));
				} else {
					errorPoi.add("unkown");
				}
				continue;
			}
			String geoNum = object.getString("geo_num");
			String level = object.getString("geo_level");
			int iLevel = Integer.parseInt(level);
			String flag = object.getString("flag");
			String typecode = object.getString("typecode");
			String id = object.getString("id");
			String index = object.getString("iwhere_index");
			int iFloor = 1;
			if (object.has("floor")) {
				iFloor = object.getInt("floor");
			}
			short sIndex = Short.parseShort(index);
			byte[] rowkey = null;
			try {
				rowkey = HBaseRowkeyUtil.getRowkeyIwhereId(geoNum, iLevel, iFloor, typecode, sIndex);
			} catch (Exception e) {
				// TODO: handle exception
				errorPoi.add(object.getString("id"));
				continue;
			}
			// 如果flag!=0，只标注删除，并不实际删除
//			if (!flag.equals("0")) {
//				Delete delete = new Delete(rowkey);
//				deletSet.add(delete);
//				deletedPoi.add(id);
//				continue;
//			}
			Put put = new Put(rowkey);
			Timestamp time = new Timestamp(System.currentTimeMillis());
			put.addColumn(baseFamily, Bytes.toBytes("geoNum"), Bytes.toBytes(geoNum));
			put.addColumn(baseFamily, Bytes.toBytes("level"), Bytes.toBytes(level));
			put.addColumn(baseFamily, Bytes.toBytes("flag"), Bytes.toBytes(flag));
			put.addColumn(baseFamily, Bytes.toBytes("typecode"), Bytes.toBytes(typecode));
			put.addColumn(baseFamily, Bytes.toBytes("id"), Bytes.toBytes(id));
			put.addColumn(baseFamily, Bytes.toBytes("update_time"), Bytes.toBytes(time.toString()));
			// 处理base字段
			if (object.has("base")) {
				JSONObject base = object.getJSONObject("base");
				Iterator iterator = base.keys();
				while (iterator.hasNext()) {
					String key = (String) iterator.next();
					String value = base.getString(key);
					put.addColumn(baseFamily, Bytes.toBytes(key), Bytes.toBytes(value));
				}
			}
			// 处理extra字段
			if (object.has("extra")) {
				JSONObject extra = object.getJSONObject("extra");
				Iterator iterator = extra.keys();
				{
					while (iterator.hasNext()) {
						String key = (String) iterator.next();
						put.addColumn(extraFamily, Bytes.toBytes(key), Bytes.toBytes(extra.getString(key)));
					}
				}
			}
			putsSet.add(put);
			addedPoi.add(id);
		}
		try {
			if (!putsSet.isEmpty()) {
				table.put(putsSet);
			}
			if (!deletedPoi.isEmpty()) {
				table.delete(deletSet);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}
		//reMap = BllConstantEnum.getReturnByError(BllConstantEnum.RESCODE_0);
		reMap.put("addNum", addedPoi.size());
		reMap.put("addList", addedPoi.toString());
		reMap.put("deleteNum", deletedPoi.size());
		reMap.put("deleteList", deletedPoi.toString());
		reMap.put("errorNum", errorPoi.size());
		reMap.put("errorList", errorPoi.toString());
		return reMap;
	}

	/**
	 *
	 * Phoenix方式的数据入库
	 * @param jsonArray poi数据
	 * @param lan 语言：0中文库 1英文库
	 * @return
	 */
	public JSONObject updateBatchWithPhoenix(JSONArray jsonArray,Integer lan) {

		String tableName = null;
		JSONObject re = new JSONObject();
		Table table = null;
		if(lan==1){
			tableName=iwhereGoTableZh;
		}else {
			tableName=iwhereGoTableEn;
		}
		try {;
			table = HBaseConManager.getInstance().getHTableInstanceByName(1, tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		List<String> addedPoi = new ArrayList<>();
		List<String> deletedPoi = new ArrayList<>();
		List<String> errorPoi = new ArrayList<>();
		List<Delete> deletSet = new ArrayList<>();
		List<Put> putsSet = new ArrayList<>();
		for (int i = 0; i < jsonArray.size(); ++i) {
			JSONObject object = (JSONObject) jsonArray.get(i);

			if (!(object.has("geo_num") & object.has("geo_level") & object.has("flag") & object.has("typecode")
					& object.has("id") & object.has("iwhere_index"))) {
				if (object.has("id")) {
					errorPoi.add(object.getString("id"));
				} else {
					errorPoi.add("unkown");
				}
				continue;
			}

			String geoNum = object.getString("geo_num");
			String level = object.getString("geo_level");
			int iLevel = Integer.parseInt(level);
			String flag = object.getString("flag");
			String typecode = object.getString("typecode");
			String id = object.getString("id");
			String index = object.getString("iwhere_index");
			int iFloor = 1;
			if (object.has("floor")) {
				iFloor = object.getInt("floor");
			}
			short sIndex = Short.parseShort(index);
			byte[] rowkey = null;
			try {
				rowkey = HBaseRowkeyUtil.getRowkeyIwhereId(geoNum, iLevel, iFloor, typecode, sIndex);
			} catch (Exception e) {
				// TODO: handle exception
				errorPoi.add(object.getString("id"));
				continue;
			}

			Put put = new Put(rowkey);
			Timestamp time = new Timestamp(System.currentTimeMillis());
			put.addColumn(baseFamily, Bytes.toBytes("geoNum"), Bytes.toBytes(geoNum));
			put.addColumn(baseFamily, Bytes.toBytes("level"), Bytes.toBytes(level));
			put.addColumn(baseFamily, Bytes.toBytes("flag"), Bytes.toBytes(flag));
			put.addColumn(baseFamily, Bytes.toBytes("typecode"), Bytes.toBytes(typecode));
			put.addColumn(baseFamily, Bytes.toBytes("id"), Bytes.toBytes(id));
			put.addColumn(baseFamily, Bytes.toBytes("update_time"), Bytes.toBytes(time.toString()));
			// 处理base字段
			if (object.has("base")) {
				JSONObject base = object.getJSONObject("base");
				Iterator iterator = base.keys();
				while (iterator.hasNext()) {
					String key = (String) iterator.next();
					String value = base.getString(key);
					put.addColumn(baseFamily, Bytes.toBytes(key), Bytes.toBytes(value));
				}
			}
			// 处理extra字段
			if (object.has("extra")) {
				JSONObject extra = object.getJSONObject("extra");
				Iterator iterator = extra.keys();
				{
					while (iterator.hasNext()) {
						String key = (String) iterator.next();
						put.addColumn(extraFamily, Bytes.toBytes(key), Bytes.toBytes(extra.getString(key)));
					}
				}
			}
			putsSet.add(put);
			addedPoi.add(id);
		}
		try {
			if (!putsSet.isEmpty()) {
				table.put(putsSet);
			}
			if (!deletedPoi.isEmpty()) {
				table.delete(deletSet);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();

			return null;
		}
		re.put("addNum", addedPoi.size());
		re.put("addList", addedPoi.toString());
		re.put("deleteNum", deletedPoi.size());
		re.put("deleteList", deletedPoi.toString());
		re.put("errorNum", errorPoi.size());
		re.put("errorList", errorPoi.toString());

		return re;
	}
}
