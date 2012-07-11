/**
 * 
 */
package com.taobao.tfstest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author Administrator
 *
 */
public class Function_test extends FailOverBaseCase {
	@Test
	public void test_01 (){
		boolean bRet = false;
		bRet = chkBlockCnt(500, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(500, 3);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(500, 0);
		Assert.assertTrue(bRet);
	}
	
	@Before
	public void setUp(){
		boolean bRet = false;
		
		/* Set Vip */
		bRet = migrateVip();
		Assert.assertTrue(bRet);
		
		/* Kill the client process */
		bRet = tfsSeedClient.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);
		
		/* Kill the grid */
		bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);
		
		/* Clean the log file */
		bRet = tfsGrid.clean();
		Assert.assertTrue(bRet);
		
		bRet = tfsGrid.start();
		Assert.assertTrue(bRet);
		
		/* Set failcount */
		bRet = resetAllFailCnt();
		Assert.assertTrue(bRet);
	
	}
}
