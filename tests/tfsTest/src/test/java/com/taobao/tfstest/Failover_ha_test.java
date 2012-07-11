/**
 * 
 */
package com.taobao.tfstest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author Administrator
 *
 */
public class Failover_ha_test extends FailOverBaseCase {
	
	//@Test
	public void Failover_XX_ns_restart_without_meta_data(){
		/* Do nothing */
		return ;
	}

	//@Test
	public void Failover_01_ns_ha_switch_once(){
		
		boolean bRet = false;
		caseName = "Failover_01_ns_ha_switch_once";
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
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
	
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Restart ns */
		bRet = startSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	//@Test
	public void Failover_02_ns_ha_switch_serval_times(){
		
		boolean bRet = false;
		caseName = "Failover_02_ns_ha_switch_serval_times";
		log.info(caseName + "===> start");
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			/* Kill master ns */
			bRet = killMasterNs();
			Assert.assertTrue(bRet);
			
			sleep(20);
			
			/* Restart ns */
			bRet = startSlaveNs();
			Assert.assertTrue(bRet);
		}
		
		/* Wait 20s for recover */
		sleep (10);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	//@Test
	public void Failover_03_ns_ha_slave_restart_once(){
		
		boolean bRet = false;
		caseName = "Failover_03_ns_ha_slave_restart_once";
		log.info(caseName + "===> start");
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Start slave ns */
		bRet = startSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Failover_04_ns_ha_slave_restart_serval_times(){
		
		boolean bRet = false;
		caseName = "Failover_04_ns_ha_slave_restart_serval_times";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
	
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			/* Kill master ns */
			bRet = killSlaveNs();
			Assert.assertTrue(bRet);
			
			sleep(20);
			
			/* Start slave ns */
			bRet = startSlaveNs();
			Assert.assertTrue(bRet);
		}
		
		/* Wait 20s for recover */
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Failover_05_ns_ha_ns_restart_once(){
		
		boolean bRet = false;
		caseName = "Failover_05_ns_ha_ns_restart_once";
		log.info(caseName + "===> start");
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		sleep(20);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Start slave ns */
		bRet = startSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Wait 60s for recover */
		sleep (60);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_06_ns_ha_ns_restart_serval_times(){
		
		boolean bRet = false;
		caseName = "Failover_06_ns_ha_ns_restart_serval_times";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			/* Kill master ns */
			bRet = killMasterNs();
			Assert.assertTrue(bRet);
			
			sleep(20);
			
			/* Kill master ns */
			bRet = killMasterNs();
			Assert.assertTrue(bRet);
		}
		
		/* Start slave ns */
		bRet = startSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Wait 60s for recover */
		sleep (60);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_07_ns_ha_slave_down(){
		
		boolean bRet = false;
		caseName = "Failover_07_ns_ha_slave_down";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
public void Failover_08_one_ds_out_copy_block_switch(){
		
		boolean bRet = false;
		caseName = "Failover_08_one_ds_out_copy_block_switch";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
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

	public void Failover_09_one_ds_in_switch(){
		
		boolean bRet = false;
		caseName = "Failover_09_one_ds_in_switch";
		log.info(caseName + "===> start");
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (30);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* One ds in */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
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
	
	public void Failover_10_compact_switch(){
		
		boolean bRet = false;
		caseName = "Failover_10_compact_switch";
		log.info(caseName + "===> start");
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(70);
		Assert.assertTrue(bRet);	
		
		/* Start write cmd */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		sleep (400);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Wait for compact */
		sleep(300);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the final rate */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_11_ns_ha_switch_once_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_11_ns_ha_switch_once_without_press";
		log.info(caseName + "===> start");
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Restart ns */
		bRet = startSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_12_ns_ha_switch_serval_times_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_12_ns_ha_switch_serval_times_without_press";
		log.info(caseName + "===> start");
		
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			/* Kill master ns */
			bRet = killMasterNs();
			Assert.assertTrue(bRet);
			
			sleep(20);
			
			/* Restart ns */
			bRet = startSlaveNs();
			Assert.assertTrue(bRet);
		}
		
		/* Wait 20s for recover */
		sleep (10);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_13_ns_ha_slave_restart_once_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_13_ns_ha_slave_restart_once_without_press";
		log.info(caseName + "===> start");
		
		/* Put the seed */
		bRet = putSeed();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (10);
		
		/* Start slave ns */
		bRet = startSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Failover_14_ns_ha_slave_restart_serval_times_whitout_press(){
		
		boolean bRet = false;
		caseName = "Failover_14_ns_ha_slave_restart_serval_times_whitout_press";
		log.info(caseName + "===> start");
		
		/* Put seed */
		bRet = putSeed();
		Assert.assertTrue(bRet);
		
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			/* Kill master ns */
			bRet = killSlaveNs();
			Assert.assertTrue(bRet);
			
			sleep(20);
			
			/* Start slave ns */
			bRet = startSlaveNs();
			Assert.assertTrue(bRet);
		}
		
		/* Wait 20s for recover */
		sleep (10);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_15_ns_ha_ns_restart_once_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_15_ns_ha_ns_restart_once_without_press";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = putSeed();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killSlaveNs();
		Assert.assertTrue(bRet);
		
		sleep(20);
		
		/* Kill master ns */
		bRet = killSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_16_ns_ha_ns_restart_serval_times_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_16_ns_ha_ns_restart_serval_times_without_press";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = putSeed();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		for (int iLoop = 0; iLoop < 10; iLoop ++)
		{
			/* Kill master ns */
			bRet = killSlaveNs();
			Assert.assertTrue(bRet);
			
			sleep(20);
			
			/* Kill master ns */
			bRet = killSlaveNs();
			Assert.assertTrue(bRet);
		}
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_17_ns_ha_slave_down_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_17_ns_ha_slave_down_without_press";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = putSeed();	
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killSlaveNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);
		
		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
public void Failover_18_one_ds_out_copy_block_switch_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_18_one_ds_out_copy_block_switch_without_press";
		log.info(caseName + "===> start");
		
		/* Write file */
		bRet = putSeed();	
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Failover_19_one_ds_in_switch_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_19_one_ds_in_switch_without_press";
		log.info(caseName + "===> start");
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = putSeed();
		Assert.assertTrue(bRet);
		
		/* One ds in */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
	
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Failover_20_compact_switch_without_press(){
		
		boolean bRet = false;
		caseName = "Failover_20_compact_switch_without_press";
		log.info(caseName + "===> start");
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(70);
		Assert.assertTrue(bRet);
		
		/* Start write cmd */
		bRet = putSeed();
		Assert.assertTrue(bRet);
		
		/* Wait for compact */
		sleep(300);
		
		/* Kill master ns */
		bRet = killMasterNs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the final rate */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	//@After
	public void tearDown(){
		boolean bRet = false;
		
		/* Stop all client process */
		bRet = allCmdStop();
		Assert.assertTrue(bRet);
		
		/* Move the seed file list */
		bRet = mvSeedFile();
		Assert.assertTrue(bRet);
		
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
