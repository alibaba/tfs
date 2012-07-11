/**
 * 
 */
package com.taobao.tfstest;

import java.util.ArrayList;
import java.util.HashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author mingyan
 *
 */
public class Function_ns_added_since_133_test extends FailOverBaseCase {
	
	//@Test
	public void Function_01_one_ds_out_emerge_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_01_one_ds_out_emerge_rep_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
	
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait 10s for recover */
		sleep (10);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_EMERG_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Check multi replicated block */
		bRet = chkMultiReplicatedBlock();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_02_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_02_rep_block";
		log.info(caseName + "===> start");
		
		/* Modify MinReplication */
		bRet = setMinReplication(1);
		Assert.assertTrue(bRet);
		
		/* Modify MaxReplication */
		bRet = setMaxReplication(1);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(1);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
	
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Modify MaxReplication */
		bRet = setMaxReplication(3);
		Assert.assertTrue(bRet);
		
		/* Wait 10s for copy */
		sleep (10);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(2);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Modify MinReplication */
		bRet = setMinReplication(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);	
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_03_move_block(){
		
		boolean bRet = false;
		caseName = "Function_03_move_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(100);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Modify balance_max_diff_block_num */
		bRet = setBalanceMaxDiffBlockNum(1);
		Assert.assertTrue(bRet);
		
		/* Wait 120s for write */
		sleep (120);

		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_MOVE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_04_compact_block(){
		
		boolean bRet = false;
		caseName = "Function_04_compact_block";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Compact block */
		bRet = compactBlock();
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_COMPACT, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);	
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_05_delete_block(){
		
		boolean bRet = false;
		caseName = "Function_05_delete_block";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait 10s for ssm to update the latest info */
		sleep (10);
		
		/* Wait for completion of replication */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Start the killed ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);

		/* Wait 10s for ssm to update the latest info */
		sleep (10);
		
		/* Wait for completion of deletion */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_DELETE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_06_one_ds_out_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_06_one_ds_out_clear_plan";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Check interrupt */
		bRet = checkInterrupt(1);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_07_one_ds_join_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_07_one_ds_join_clear_plan";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);

		/* Start the 1st ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Check interrupt */
		bRet = checkInterrupt(1);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_08_one_ds_restart_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_08_one_ds_restart_clear_plan";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Start the 1st ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
	
		/* Wait */
		sleep(10);	
		
		/* Check interrupt */
		bRet = checkInterrupt(2);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_09_new_elect_writable_block_algorithm(){
		
		boolean bRet = false;
		caseName = "Function_09_new_elect_writable_block_algorithm";
		log.info(caseName + "===> start");

		/* Wait */
		sleep(60);	
		
		/* Get the block num hold by each ds before write */
		HashMap<String, Integer> blockDisBefore = new HashMap<String, Integer>();
		bRet = getBlockDistribution(blockDisBefore);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Start sar to account network traffic */
		ArrayList<String> ds_ip_list = new ArrayList<String>();
		bRet = networkTrafMonStart(SAMPLE_INTERVAL, TEST_TIME/SAMPLE_INTERVAL, ds_ip_list);
		Assert.assertTrue(bRet);
	
		/* Wait */
		sleep(TEST_TIME);	
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY);
		Assert.assertTrue(bRet);

		/* Wait for completion of replication */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);

		/* Check network traffic balance */
		bRet = chkNetworkTrafBalance("eth0", RXBYTPERSEC_SD_COL, ds_ip_list);
		Assert.assertTrue(bRet);
		
		/* Get the block num hold by each ds after write */
		HashMap<String, Integer> blockDisAfter = new HashMap<String, Integer>();
		bRet = getBlockDistribution(blockDisAfter);
		Assert.assertTrue(bRet);
		
		/* Check new added block balance status*/
		bRet = checkWriteBalanceStatus(blockDisBefore, blockDisAfter);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_10_check_read_network_traffic_balance(){
		
		boolean bRet = false;
		caseName = "Function_10_check_read_network_traffic_balance";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep(20);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Set loop flag */
		bRet = setReadFlag(LOOPON);
		Assert.assertTrue(bRet);	
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Start sar to account network traffic */
		ArrayList<String> ds_ip_list = new ArrayList<String>();
		bRet = networkTrafMonStart(SAMPLE_INTERVAL, TEST_TIME/SAMPLE_INTERVAL, ds_ip_list);
		Assert.assertTrue(bRet);
		
		/* Wait */
		sleep(TEST_TIME);	
		
		/* Stop cmd */
		bRet = readCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);

		/* Check network traffic balance */
		bRet = chkNetworkTrafBalance("eth0", TXBYTPERSEC_SD_COL, ds_ip_list);
		Assert.assertTrue(bRet);
		
		/* Set loop flag */
		bRet = setReadFlag(LOOPOFF);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}	
	
	public void Function_11_modify_gc_wait_time(){
		
		boolean bRet = false;
		caseName = "Function_11_modify_gc_wait_time";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(10);
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);

		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(7200);
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);

		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(3600);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}	
	
	@After
	public void tearDown(){
		boolean bRet = false;
		
		/* Move the seed file list */
//		bRet = mvSeedFile();
//		Assert.assertTrue(bRet);
		
		/* Clean the caseName */
		caseName = "";
	}
	
	@Before
	public void setUp(){
		boolean bRet = false;
		
		/* Reset case name */
		caseName = "";

		/* Set the failcount */
		bRet = setAllFailCnt();
		Assert.assertTrue(bRet);
		
		/* Set Vip */
		bRet = migrateVip();
		Assert.assertTrue(bRet);
		
		/* Kill the grid */
		bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);
		
		/* Set Vip */
		bRet = migrateVip();
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
