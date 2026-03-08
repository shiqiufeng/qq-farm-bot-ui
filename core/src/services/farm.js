/**
 * 自己的农场操作 - 收获/浇水/除草/除虫/铲除/种植/商店/巡田
 */

const protobuf = require('protobufjs');
const { CONFIG, PlantPhase, PHASE_NAMES } = require('../config/config');
const { getPlantNameBySeedId, getPlantName, getPlantExp, formatGrowTime, getPlantGrowTime, getAllSeeds, getPlantById, getSeedImageBySeedId } = require('../config/gameConfig');
const { isAutomationOn, getPreferredSeed, getAutomation, getPlantingStrategy, getOrganicAntiStealMinutes } = require('../models/store');
const { sendMsgAsync, getUserState, networkEvents, getWsErrorState } = require('../utils/network');
const { types } = require('../utils/proto');
const { toLong, toNum, getServerTimeSec, toTimeSec, log, logWarn, sleep } = require('../utils/utils');
const { getPlantRankings } = require('./analytics');
const { createScheduler } = require('./scheduler');
const { recordOperation } = require('./stats');
const { getFarmOptimizer } = require('./rate-limiter');

// ============ 内部状态 ============
let isCheckingFarm = false;
let isFirstFarmCheck = true;
let farmLoopRunning = false;
let externalSchedulerMode = false;
const farmScheduler = createScheduler('farm');

// ============ 农场 API ============

// 操作限制更新回调 (由 friend.js 设置)
let onOperationLimitsUpdate = null;
function setOperationLimitsCallback(callback) {
    onOperationLimitsUpdate = callback;
}

/**
 * 通用植物操作请求
 */
async function sendPlantRequest(RequestType, ReplyType, method, landIds, hostGid) {
    const body = RequestType.encode(RequestType.create({
        land_ids: landIds,
        host_gid: toLong(hostGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', method, body);
    return ReplyType.decode(replyBody);
}

async function getAllLands() {
    const body = types.AllLandsRequest.encode(types.AllLandsRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'AllLands', body);
    const reply = types.AllLandsReply.decode(replyBody);
    // 更新操作限制
    if (reply.operation_limits && onOperationLimitsUpdate) {
        onOperationLimitsUpdate(reply.operation_limits);
    }
    return reply;
}

async function harvest(landIds) {
    const state = getUserState();
    const body = types.HarvestRequest.encode(types.HarvestRequest.create({
        land_ids: landIds,
        host_gid: toLong(state.gid),
        is_all: true,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Harvest', body);
    return types.HarvestReply.decode(replyBody);
}

async function waterLand(landIds) {
    const state = getUserState();
    return sendPlantRequest(types.WaterLandRequest, types.WaterLandReply, 'WaterLand', landIds, state.gid);
}

async function weedOut(landIds) {
    const state = getUserState();
    return sendPlantRequest(types.WeedOutRequest, types.WeedOutReply, 'WeedOut', landIds, state.gid);
}

async function insecticide(landIds) {
    const state = getUserState();
    return sendPlantRequest(types.InsecticideRequest, types.InsecticideReply, 'Insecticide', landIds, state.gid);
}

// 普通肥料 ID
const NORMAL_FERTILIZER_ID = 1011;
// 有机肥料 ID
const ORGANIC_FERTILIZER_ID = 1012;

/**
 * 施肥 - 必须逐块进行，服务器不支持批量
 * 游戏中拖动施肥间隔很短，这里用 50ms
 */
async function fertilize(landIds, fertilizerId = NORMAL_FERTILIZER_ID) {
    let successCount = 0;
    for (const landId of landIds) {
        try {
            const body = types.FertilizeRequest.encode(types.FertilizeRequest.create({
                land_ids: [toLong(landId)],
                fertilizer_id: toLong(fertilizerId),
            })).finish();
            await sendMsgAsync('gamepb.plantpb.PlantService', 'Fertilize', body);
            successCount++;
        } catch {
            // 施肥失败（可能肥料不足），停止继续
            break;
        }
        if (landIds.length > 1) await sleep(50);  // 50ms 间隔
    }
    return successCount;
}

/**
 * 有机肥循环施肥:
 * 按地块顺序 1-2-3-...-1 持续施肥，直到出现失败即停止。
 */
async function fertilizeOrganicLoop(landIds) {
    const ids = (Array.isArray(landIds) ? landIds : []).filter(Boolean);
    if (ids.length === 0) return 0;

    let successCount = 0;
    let idx = 0;

    while (true) {
        const landId = ids[idx];
        try {
            const body = types.FertilizeRequest.encode(types.FertilizeRequest.create({
                land_ids: [toLong(landId)],
                fertilizer_id: toLong(ORGANIC_FERTILIZER_ID),
            })).finish();
            await sendMsgAsync('gamepb.plantpb.PlantService', 'Fertilize', body);
            successCount++;
        } catch {
            // 常见是有机肥耗尽，按需求直接停止
            break;
        }

        idx = (idx + 1) % ids.length;
        await sleep(1000);
    }

    return successCount;
}

function getOrganicFertilizerTargetsFromLands(lands) {
    const list = Array.isArray(lands) ? lands : [];
    const targets = [];
    for (const land of list) {
        if (!land || !land.unlocked) continue;
        const landId = toNum(land.id);
        if (!landId) continue;

        const plant = land.plant;
        if (!plant || !plant.phases || plant.phases.length === 0) continue;
        const currentPhase = getCurrentPhase(plant.phases);
        if (!currentPhase) continue;
        if (currentPhase.phase === PlantPhase.DEAD) continue;

        // 服务端有该字段时，<=0 说明该地当前不能再施有机肥
        if (Object.prototype.hasOwnProperty.call(plant, 'left_inorc_fert_times')) {
            const leftTimes = toNum(plant.left_inorc_fert_times);
            if (leftTimes <= 0) continue;
        }

        targets.push(landId);
    }
    return targets;
}

async function runFertilizerByConfig(plantedLands = []) {
    const fertilizerConfig = getAutomation().fertilizer || 'both';
    const planted = (Array.isArray(plantedLands) ? plantedLands : []).filter(Boolean);

    if (planted.length === 0 && fertilizerConfig !== 'organic' && fertilizerConfig !== 'both') {
        return { normal: 0, organic: 0 };
    }

    let fertilizedNormal = 0;
    let fertilizedOrganic = 0;

    if ((fertilizerConfig === 'normal' || fertilizerConfig === 'both') && planted.length > 0) {
        fertilizedNormal = await fertilize(planted, NORMAL_FERTILIZER_ID);
        if (fertilizedNormal > 0) {
            log('施肥', `已为 ${fertilizedNormal}/${planted.length} 块地施无机化肥`, {
                module: 'farm',
                event: 'fertilize',
                result: 'ok',
                type: 'normal',
                count: fertilizedNormal,
            });
            recordOperation('fertilize', fertilizedNormal);
        }
    }

    if (fertilizerConfig === 'organic' || fertilizerConfig === 'both') {
        let organicTargets = planted;
        try {
            const latest = await getAllLands();
            organicTargets = getOrganicFertilizerTargetsFromLands(latest && latest.lands);
        } catch (e) {
            logWarn('施肥', `获取全农场地块失败，回退已种地块: ${e.message}`);
        }

        fertilizedOrganic = await fertilizeOrganicLoop(organicTargets);
        if (fertilizedOrganic > 0) {
            log('施肥', `有机化肥循环施肥完成，共施 ${fertilizedOrganic} 次`, {
                module: 'farm',
                event: 'fertilize',
                result: 'ok',
                type: 'organic',
                count: fertilizedOrganic,
            });
            recordOperation('fertilize', fertilizedOrganic);
        }
    }

    return { normal: fertilizedNormal, organic: fertilizedOrganic };
}

/**
 * 有机肥防偷功能
 * 当作物即将成熟时，使用有机肥催熟并立即收获，防止被好友偷菜
 * @returns {Promise<{fertilized: number, harvested: number}>} 施肥和收获的数量
 */
async function runOrganicAntiSteal() {
    // 检查有机肥防偷开关是否开启
    const organicAntiStealEnabled = isAutomationOn('organicAntiSteal');

    if (!organicAntiStealEnabled) {
        return { fertilized: 0, harvested: 0 };
    }

    // 获取提前分钟数配置，转换为秒
    const thresholdMinutes = getOrganicAntiStealMinutes();
    const ANTI_STEAL_THRESHOLD_SEC = thresholdMinutes * 60;
    const nowSec = getServerTimeSec();
    let fertilizedCount = 0;
    let harvestedCount = 0;

    // 获取所有地块信息
    let latestLands;
    try {
        latestLands = await getAllLands();
    } catch (e) {
        logWarn('有机肥防偷', `获取地块数据失败: ${e.message}`);
        return { fertilized: 0, harvested: 0 };
    }

    if (!latestLands || !latestLands.lands) {
        return { fertilized: 0, harvested: 0 };
    }

    const lands = latestLands.lands;
    const antiStealTargets = [];

    // 遍历所有地块，筛选出需要防偷的目标
    for (const land of lands) {
        // 跳过未解锁的地块
        if (!land || !land.unlocked) continue;
        const landId = toNum(land.id);
        if (!landId) continue;

        const plant = land.plant;
        // 跳过没有作物的地块
        if (!plant || !plant.phases || plant.phases.length === 0) {
            continue;
        }

        const plantId = toNum(plant.id);
        const plantName = getPlantName(plantId) || plant.name || '未知作物';

        const currentPhase = getCurrentPhase(plant.phases);
        if (!currentPhase) continue;
        // 跳过枯死的作物
        if (currentPhase.phase === PlantPhase.DEAD) continue;
        // 跳过已成熟的作物
        if (currentPhase.phase === PlantPhase.MATURE) continue;

        // 检查是否还能施有机肥（服务器限制）
        if (Object.prototype.hasOwnProperty.call(plant, 'left_inorc_fert_times')) {
            const leftTimes = toNum(plant.left_inorc_fert_times);
            if (leftTimes <= 0) continue;
        }

        // 查找成熟阶段，计算距离成熟的时间
        const maturePhase = plant.phases.find((p) => p && toNum(p.phase) === PlantPhase.MATURE);
        if (!maturePhase) continue;

        const matureBegin = toTimeSec(maturePhase.begin_time);
        const matureInSec = matureBegin > nowSec ? (matureBegin - nowSec) : 0;

        // 如果距离成熟时间在阈值范围内，加入防偷目标列表
        if (matureInSec > 0 && matureInSec <= ANTI_STEAL_THRESHOLD_SEC) {
            const matureInMin = Math.ceil(matureInSec / 60);
            antiStealTargets.push({ landId, plantName, matureInSec, matureInMin });
        }
    }

    // 如果没有需要防偷的地块，直接返回
    if (antiStealTargets.length === 0) {
        return { fertilized: 0, harvested: 0 };
    }

    // 记录日志：发现需要防偷的地块
    const targetsSummary = antiStealTargets.map(t => `#${t.landId}(${t.matureInMin}分钟)`).join(', ');
    log('有机肥防偷', `发现 ${antiStealTargets.length} 块地需要防偷: ${targetsSummary}，开始施有机肥...`, {
        module: 'farm',
        event: '有机肥防偷_开始施肥',
        count: antiStealTargets.length,
        targets: targetsSummary,
    });

    let organicFertilizerEmpty = false;
    const fertilizedLands = [];

    // 逐个地块施有机肥
    for (const target of antiStealTargets) {
        try {
            // 构造施肥请求
            const body = types.FertilizeRequest.encode(types.FertilizeRequest.create({
                land_ids: [toLong(target.landId)],
                fertilizer_id: toLong(ORGANIC_FERTILIZER_ID),
            })).finish();
            await sendMsgAsync('gamepb.plantpb.PlantService', 'Fertilize', body);
            fertilizedCount++;
            fertilizedLands.push(`#${target.landId}(${target.plantName})`);
        } catch (e) {
            // 错误码 1000019 表示有机肥不足
            if (e.message && e.message.includes('1000019')) {
                if (!organicFertilizerEmpty) {
                    logWarn('有机肥防偷', '有机化肥不足，无法继续防偷');
                    organicFertilizerEmpty = true;
                }
            } else {
                logWarn('有机肥防偷', `地块 #${target.landId} 施肥失败: ${e.message}`);
            }
            break;
        }
        // 多个地块时，每次施肥间隔 50ms
        if (antiStealTargets.length > 1) await sleep(50);
    }

    // 如果没有成功施肥的地块，直接返回
    if (fertilizedCount === 0) {
        return { fertilized: 0, harvested: 0 };
    }

    if (fertilizedCount > 0) {
        // 记录施肥完成日志
        const fertilizedSummary = fertilizedLands.join(', ');
        log('有机肥防偷', `施肥完成，成功 ${fertilizedCount}/${antiStealTargets.length} 块: ${fertilizedSummary}，等待服务器更新...`, {
            module: 'farm',
            event: '有机肥防偷_施肥完成',
            count: fertilizedCount,
            lands: fertilizedSummary,
        });
        recordOperation('fertilize', fertilizedCount);

        // 等待服务器处理施肥请求毫秒
        await sleep(30);

        log('有机肥防偷', '开始检查成熟状态并收获...', {
            module: 'farm',
            event: '有机肥防偷_开始收获',
        });

        try {
            // 重新获取地块信息，检查成熟状态
            const afterLands = await getAllLands();
            if (afterLands && afterLands.lands) {
                const harvestableLands = [];
                // 遍历所有地块，找出已成熟的作物
                for (const land of afterLands.lands) {
                    if (!land || !land.unlocked) continue;
                    const plant = land.plant;
                    if (!plant || !plant.phases) continue;
                    const currentPhase = getCurrentPhase(plant.phases);
                    // 只收获成熟阶段的作物
                    if (currentPhase && currentPhase.phase === PlantPhase.MATURE) {
                        const landId = toNum(land.id);
                        const plantId = toNum(plant.id);
                        const plantName = getPlantName(plantId) || plant.name || '未知作物';
                        harvestableLands.push({ landId, plantName });
                    }
                }

                // 如果有成熟的作物，执行收获
                if (harvestableLands.length > 0) {
                    // 提取所有成熟地块的 ID 列表
                    const landIds = harvestableLands.map(h => h.landId);
                    
                    // 调用收获接口，批量收获所有成熟的作物
                    await harvest(landIds);
                    
                    // 记录收获的地块数量
                    harvestedCount = harvestableLands.length;
                    
                    // 格式化收获详情，用于日志显示
                    // 例如: #1(大葱), #2(大葱), #3(大葱)
                    const details = harvestableLands.map(h => `#${h.landId}(${h.plantName})`).join(', ');
                    
                    // 记录收获成功的日志
                    log('有机肥防偷', `收获完成！共收获 ${harvestedCount} 块地: ${details}`, {
                        module: 'farm',
                        event: '有机肥防偷_收获成功',
                        count: harvestedCount,
                        lands: landIds,
                    });
                    
                    // 记录防偷操作次数（用于今日统计）
                    recordOperation('antiSteal', harvestedCount);
                    
                    // 记录收获操作次数（用于今日统计）
                    recordOperation('harvest', harvestedCount);
                } else {
                    // 如果施肥后没有发现成熟的作物，说明施肥可能没有生效
                    // 可能原因：有机肥不足、服务器延迟、网络问题等
                    logWarn('有机肥防偷', '施肥后未发现成熟作物，可能施肥未生效');
                }
            }
        } catch (e) {
            logWarn('有机肥防偷', `收获失败: ${e.message}`);
        }
    }

    return { fertilized: fertilizedCount, harvested: harvestedCount };
}

async function removePlant(landIds) {
    const body = types.RemovePlantRequest.encode(types.RemovePlantRequest.create({
        land_ids: landIds.map(id => toLong(id)),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'RemovePlant', body);
    return types.RemovePlantReply.decode(replyBody);
}

async function upgradeLand(landId) {
    const body = types.UpgradeLandRequest.encode(types.UpgradeLandRequest.create({
        land_id: toLong(landId),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'UpgradeLand', body);
    return types.UpgradeLandReply.decode(replyBody);
}

async function unlockLand(landId, doShared = false) {
    const body = types.UnlockLandRequest.encode(types.UnlockLandRequest.create({
        land_id: toLong(landId),
        do_shared: !!doShared,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'UnlockLand', body);
    return types.UnlockLandReply.decode(replyBody);
}

// ============ 商店 API ============

async function getShopInfo(shopId) {
    const body = types.ShopInfoRequest.encode(types.ShopInfoRequest.create({
        shop_id: toLong(shopId),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.shoppb.ShopService', 'ShopInfo', body);
    return types.ShopInfoReply.decode(replyBody);
}

async function buyGoods(goodsId, num, price) {
    const body = types.BuyGoodsRequest.encode(types.BuyGoodsRequest.create({
        goods_id: toLong(goodsId),
        num: toLong(num),
        price: toLong(price),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.shoppb.ShopService', 'BuyGoods', body);
    return types.BuyGoodsReply.decode(replyBody);
}

// ============ 种植 ============

function encodePlantRequest(seedId, landIds) {
    const writer = protobuf.Writer.create();
    const itemWriter = writer.uint32(18).fork();
    itemWriter.uint32(8).int64(seedId);
    const idsWriter = itemWriter.uint32(18).fork();
    for (const id of landIds) {
        idsWriter.int64(id);
    }
    idsWriter.ldelim();
    itemWriter.ldelim();
    return writer.finish();
}

/**
 * 种植 - 游戏中拖动种植间隔很短，这里用 50ms
 */
async function plantSeeds(seedId, landIds) {
    let successCount = 0;
    for (const landId of landIds) {
        try {
            const body = encodePlantRequest(seedId, [landId]);
            const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Plant', body);
            types.PlantReply.decode(replyBody);
            successCount++;
        } catch (e) {
            logWarn('种植', `土地#${landId} 失败: ${e.message}`);
        }
        if (landIds.length > 1) await sleep(50);  // 50ms 间隔
    }
    return successCount;
}

async function findBestSeed() {
    const SEED_SHOP_ID = 2;
    const shopReply = await getShopInfo(SEED_SHOP_ID);
    if (!shopReply.goods_list || shopReply.goods_list.length === 0) {
        logWarn('商店', '种子商店无商品');
        return null;
    }

    const state = getUserState();
    const available = [];
    for (const goods of shopReply.goods_list) {
        if (!goods.unlocked) continue;

        let meetsConditions = true;
        let requiredLevel = 0;
        const conds = goods.conds || [];
        for (const cond of conds) {
            if (toNum(cond.type) === 1) {
                requiredLevel = toNum(cond.param);
                if (state.level < requiredLevel) {
                    meetsConditions = false;
                    break;
                }
            }
        }
        if (!meetsConditions) continue;

        const limitCount = toNum(goods.limit_count);
        const boughtNum = toNum(goods.bought_num);
        if (limitCount > 0 && boughtNum >= limitCount) continue;

        available.push({
            goods,
            goodsId: toNum(goods.id),
            seedId: toNum(goods.item_id),
            price: toNum(goods.price),
            requiredLevel,
        });
    }

    if (available.length === 0) {
        logWarn('商店', '没有可购买的种子');
        return null;
    }

    // 按策略排序
    const strategy = getPlantingStrategy();
    const analyticsSortByMap = {
        max_exp: 'exp',
        max_fert_exp: 'fert',
        max_profit: 'profit',
        max_fert_profit: 'fert_profit',
    };
    const analyticsSortBy = analyticsSortByMap[strategy];
    if (analyticsSortBy) {
        try {
            const rankings = getPlantRankings(analyticsSortBy);
            const availableBySeedId = new Map(available.map(a => [a.seedId, a]));
            for (const row of rankings) {
                const seedId = Number(row && row.seedId) || 0;
                if (seedId <= 0) continue;
                const lv = Number(row && row.level);
                if (Number.isFinite(lv) && lv > state.level) continue;
                const found = availableBySeedId.get(seedId);
                if (found) return found;
            }
            logWarn('商店', `策略 ${strategy} 未找到可购买作物，回退最高等级`);
        } catch (e) {
            logWarn('商店', `策略 ${strategy} 计算失败: ${e.message}，回退最高等级`);
        }
        available.sort((a, b) => b.requiredLevel - a.requiredLevel);
        return available[0];
    }
    
    // 偏好模式
    if (strategy === 'preferred') {
        const preferred = getPreferredSeed();
        if (preferred > 0) {
            const found = available.find(a => a.seedId === preferred);
            if (found) return found;
            logWarn('商店', `优先种子 ${preferred} 当前不可购买，回退自动选择`);
        }
        // 如果偏好未找到或未设置，回退到默认（等级最高）
        available.sort((a, b) => b.requiredLevel - a.requiredLevel);
    }
    // 最高等级模式
    else if (strategy === 'level') {
        available.sort((a, b) => b.requiredLevel - a.requiredLevel);
    } 
    // 默认
    else {
        available.sort((a, b) => b.requiredLevel - a.requiredLevel);
    }

    return available[0];
}

async function getAvailableSeeds() {
    const SEED_SHOP_ID = 2;
    const state = getUserState();
    let list = [];
    
    try {
        const shopReply = await getShopInfo(SEED_SHOP_ID);
        if (shopReply.goods_list) {
            for (const goods of shopReply.goods_list) {
                // 不再过滤不可用的种子，而是返回给前端展示状态
                let requiredLevel = 0;
                for (const cond of goods.conds || []) {
                    if (toNum(cond.type) === 1) requiredLevel = toNum(cond.param);
                }
                
                const limitCount = toNum(goods.limit_count);
                const boughtNum = toNum(goods.bought_num);
                const isSoldOut = limitCount > 0 && boughtNum >= limitCount;
    
                list.push({
                    seedId: toNum(goods.item_id),
                    goodsId: toNum(goods.id),
                    name: getPlantNameBySeedId(toNum(goods.item_id)),
                    price: toNum(goods.price),
                    requiredLevel,
                    locked: !goods.unlocked || state.level < requiredLevel,
                    soldOut: isSoldOut,
                });
            }
        }
    } catch (e) {
        const wsErr = getWsErrorState();
        if (!wsErr || Number(wsErr.code) !== 400) {
            logWarn('商店', `获取商店失败: ${e.message}，使用本地备选列表`);
        }
    }

    // 如果商店请求失败或为空，使用本地配置
    if (list.length === 0) {
        const allSeeds = getAllSeeds();
        list = allSeeds.map(s => ({
            ...s,
            goodsId: 0,
            price: null, // 未知价格
            requiredLevel: null, // 未知等级
            unknownMeta: true,
            locked: false,
            soldOut: false,
        }));
    }
    return list.sort((a, b) => {
        const av = (a.requiredLevel === null || a.requiredLevel === undefined) ? 9999 : a.requiredLevel;
        const bv = (b.requiredLevel === null || b.requiredLevel === undefined) ? 9999 : b.requiredLevel;
        return av - bv;
    });
}

async function getLandsDetail() {
    try {
        const landsReply = await getAllLands();
        if (!landsReply.lands) return { lands: [], summary: {} };
        const status = analyzeLands(landsReply.lands);
        const nowSec = getServerTimeSec();
        const lands = [];

        for (const land of landsReply.lands) {
            const id = toNum(land.id);
            const level = toNum(land.level);
            const maxLevel = toNum(land.max_level);
            const landsLevel = toNum(land.lands_level);
            const landSize = toNum(land.land_size);
            const couldUnlock = !!land.could_unlock;
            const couldUpgrade = !!land.could_upgrade;
            if (!land.unlocked) {
                lands.push({
                    id,
                    unlocked: false,
                    status: 'locked',
                    plantName: '',
                    phaseName: '',
                    level,
                    maxLevel,
                    landsLevel,
                    landSize,
                    couldUnlock,
                    couldUpgrade,
                });
                continue;
            }
            const plant = land.plant;
            if (!plant || !plant.phases || plant.phases.length === 0) {
                lands.push({
                    id,
                    unlocked: true,
                    status: 'empty',
                    plantName: '',
                    phaseName: '空地',
                    level,
                    maxLevel,
                    landsLevel,
                    landSize,
                    couldUnlock,
                    couldUpgrade,
                });
                continue;
            }
            const currentPhase = getCurrentPhase(plant.phases, false, '');
            if (!currentPhase) {
                lands.push({
                    id,
                    unlocked: true,
                    status: 'empty',
                    plantName: '',
                    phaseName: '',
                    level,
                    maxLevel,
                    landsLevel,
                    landSize,
                    couldUnlock,
                    couldUpgrade,
                });
                continue;
            }
            const phaseVal = currentPhase.phase;
            const plantId = toNum(plant.id);
            const plantName = getPlantName(plantId) || plant.name || '未知';
            const plantCfg = getPlantById(plantId);
            const seedId = toNum(plantCfg && plantCfg.seed_id);
            const seedImage = seedId > 0 ? getSeedImageBySeedId(seedId) : '';
            const phaseName = PHASE_NAMES[phaseVal] || '';
            const maturePhase = Array.isArray(plant.phases)
                ? plant.phases.find((p) => p && toNum(p.phase) === PlantPhase.MATURE)
                : null;
            const matureBegin = maturePhase ? toTimeSec(maturePhase.begin_time) : 0;
            const matureInSec = matureBegin > nowSec ? (matureBegin - nowSec) : 0;

            let landStatus = 'growing';
            if (phaseVal === PlantPhase.MATURE) landStatus = 'harvestable';
            else if (phaseVal === PlantPhase.DEAD) landStatus = 'dead';
            else if (phaseVal === PlantPhase.UNKNOWN || !plant.phases.length) landStatus = 'empty';

            const needWater = (toNum(plant.dry_num) > 0) || (toTimeSec(currentPhase.dry_time) > 0 && toTimeSec(currentPhase.dry_time) <= nowSec);
            const needWeed = (plant.weed_owners && plant.weed_owners.length > 0) || (toTimeSec(currentPhase.weeds_time) > 0 && toTimeSec(currentPhase.weeds_time) <= nowSec);
            const needBug = (plant.insect_owners && plant.insect_owners.length > 0) || (toTimeSec(currentPhase.insect_time) > 0 && toTimeSec(currentPhase.insect_time) <= nowSec);

            lands.push({
                id,
                unlocked: true,
                status: landStatus,
                plantName,
                seedId,
                seedImage,
                phaseName,
                matureInSec,
                needWater,
                needWeed,
                needBug,
                stealable: !!plant.stealable,
                level,
                maxLevel,
                landsLevel,
                landSize,
                couldUnlock,
                couldUpgrade,
            });
        }

        return {
            lands,
            summary: {
                harvestable: status.harvestable.length,
                growing: status.growing.length,
                empty: status.empty.length,
                dead: status.dead.length,
                needWater: status.needWater.length,
                needWeed: status.needWeed.length,
                needBug: status.needBug.length,
            },
        };
    } catch {
        return { lands: [], summary: {} };
    }
}

async function autoPlantEmptyLands(deadLandIds, emptyLandIds) {
    let landsToPlant = [...emptyLandIds];
    const state = getUserState();

    // 1. 铲除枯死/收获残留植物（一键操作）
    if (deadLandIds.length > 0) {
        try {
            await removePlant(deadLandIds);
            log('铲除', `已铲除 ${deadLandIds.length} 块 (${deadLandIds.join(',')})`, {
                module: 'farm', event: 'remove_plant', result: 'ok', count: deadLandIds.length
            });
            landsToPlant.push(...deadLandIds);
        } catch (e) {
            logWarn('铲除', `批量铲除失败: ${e.message}`, {
                module: 'farm', event: 'remove_plant', result: 'error'
            });
            // 失败时仍然尝试种植
            landsToPlant.push(...deadLandIds);
        }
    }

    if (landsToPlant.length === 0) return;

    // 2. 查询种子商店
    let bestSeed;
    try {
        bestSeed = await findBestSeed();
    } catch (e) {
        logWarn('商店', `查询失败: ${e.message}`);
        return;
    }
    if (!bestSeed) return;

    const seedName = getPlantNameBySeedId(bestSeed.seedId);
    const growTime = getPlantGrowTime(1020000 + (bestSeed.seedId - 20000));  // 转换为植物ID
    const growTimeStr = growTime > 0 ? ` 生长${formatGrowTime(growTime)}` : '';
    log('商店', `最佳种子: ${seedName} (${bestSeed.seedId}) 价格=${bestSeed.price}金币${growTimeStr}`, {
        module: 'warehouse', event: 'seed_pick', seedId: bestSeed.seedId, price: bestSeed.price
    });

    // 3. 购买
    const needCount = landsToPlant.length;
    const totalCost = bestSeed.price * needCount;
    if (totalCost > state.gold) {
        logWarn('商店', `金币不足! 需要 ${totalCost} 金币, 当前 ${state.gold} 金币`, {
            module: 'farm', event: 'seed_buy_skip', result: 'insufficient_gold', need: totalCost, current: state.gold
        });
        const canBuy = Math.floor(state.gold / bestSeed.price);
        if (canBuy <= 0) return;
        landsToPlant = landsToPlant.slice(0, canBuy);
        log('商店', `金币有限，只种 ${canBuy} 块地`);
    }

    let actualSeedId = bestSeed.seedId;
    try {
        const buyReply = await buyGoods(bestSeed.goodsId, landsToPlant.length, bestSeed.price);
        if (buyReply.get_items && buyReply.get_items.length > 0) {
            const gotItem = buyReply.get_items[0];
            const gotId = toNum(gotItem.id);
            if (gotId > 0) actualSeedId = gotId;
        }
        if (buyReply.cost_items) {
            for (const item of buyReply.cost_items) {
                state.gold -= toNum(item.count);
            }
        }
        const boughtName = getPlantNameBySeedId(actualSeedId);
        log('购买', `已购买 ${boughtName}种子 x${landsToPlant.length}, 花费 ${bestSeed.price * landsToPlant.length} 金币`, {
            module: 'warehouse',
            event: 'seed_buy',
            result: 'ok',
            seedId: actualSeedId,
            count: landsToPlant.length,
            cost: bestSeed.price * landsToPlant.length,
        });
    } catch (e) {
        logWarn('购买', e.message);
        return;
    }

    // 4. 种植（逐块拖动，间隔50ms）
    let plantedLands = [];
    try {
        const planted = await plantSeeds(actualSeedId, landsToPlant);
        log('种植', `已在 ${planted} 块地种植 (${landsToPlant.join(',')})`, {
            module: 'farm',
            event: 'plant_seed',
            result: 'ok',
            seedId: actualSeedId,
            count: planted,
        });
        if (planted > 0) {
            plantedLands = landsToPlant.slice(0, planted);
        }
    } catch (e) {
        logWarn('种植', e.message);
    }

    // 5. 施肥
    await runFertilizerByConfig(plantedLands);
}

function getCurrentPhase(phases, debug, landLabel) {
    if (!phases || phases.length === 0) return null;

    const nowSec = getServerTimeSec();

    if (debug) {
        console.warn(`    ${landLabel} 服务器时间=${nowSec} (${new Date(nowSec * 1000).toLocaleTimeString()})`);
        for (let i = 0; i < phases.length; i++) {
            const p = phases[i];
            const bt = toTimeSec(p.begin_time);
            const phaseName = PHASE_NAMES[p.phase] || `阶段${p.phase}`;
            const diff = bt > 0 ? (bt - nowSec) : 0;
            const diffStr = diff > 0 ? `(未来 ${diff}s)` : diff < 0 ? `(已过 ${-diff}s)` : '';
            console.warn(`    ${landLabel}   [${i}] ${phaseName}(${p.phase}) begin=${bt} ${diffStr} dry=${toTimeSec(p.dry_time)} weed=${toTimeSec(p.weeds_time)} insect=${toTimeSec(p.insect_time)}`);
        }
    }

    for (let i = phases.length - 1; i >= 0; i--) {
        const beginTime = toTimeSec(phases[i].begin_time);
        if (beginTime > 0 && beginTime <= nowSec) {
            if (debug) {
                console.warn(`    ${landLabel}   → 当前阶段: ${PHASE_NAMES[phases[i].phase] || phases[i].phase}`);
            }
            return phases[i];
        }
    }

    if (debug) {
        console.warn(`    ${landLabel}   → 所有阶段都在未来，使用第一个: ${PHASE_NAMES[phases[0].phase] || phases[0].phase}`);
    }
    return phases[0];
}

function analyzeLands(lands) {
    const result = {
        harvestable: [], needWater: [], needWeed: [], needBug: [],
        growing: [], empty: [], dead: [], unlockable: [], upgradable: [],
        harvestableInfo: [],
    };

    const nowSec = getServerTimeSec();
    const debug = isFirstFarmCheck;

    for (const land of lands) {
        const id = toNum(land.id);
        if (!land.unlocked) {
            if (land.could_unlock) {
                result.unlockable.push(id);
            }
            continue;
        }
        if (land.could_upgrade) {
            result.upgradable.push(id);
        }

        const plant = land.plant;
        if (!plant || !plant.phases || plant.phases.length === 0) {
            result.empty.push(id);
            continue;
        }

        const plantName = plant.name || '未知作物';
        const landLabel = `土地#${id}(${plantName})`;

        const currentPhase = getCurrentPhase(plant.phases, debug, landLabel);
        if (!currentPhase) {
            result.empty.push(id);
            continue;
        }
        const phaseVal = currentPhase.phase;

        if (phaseVal === PlantPhase.DEAD) {
            result.dead.push(id);
            continue;
        }

        if (phaseVal === PlantPhase.MATURE) {
            result.harvestable.push(id);
            const plantId = toNum(plant.id);
            const plantNameFromConfig = getPlantName(plantId);
            const plantExp = getPlantExp(plantId);
            result.harvestableInfo.push({
                landId: id,
                plantId,
                name: plantNameFromConfig || plantName,
                exp: plantExp,
            });
            continue;
        }

        const dryNum = toNum(plant.dry_num);
        const dryTime = toTimeSec(currentPhase.dry_time);
        if (dryNum > 0 || (dryTime > 0 && dryTime <= nowSec)) {
            result.needWater.push(id);
        }

        const weedsTime = toTimeSec(currentPhase.weeds_time);
        const hasWeeds = (plant.weed_owners && plant.weed_owners.length > 0) || (weedsTime > 0 && weedsTime <= nowSec);
        if (hasWeeds) {
            result.needWeed.push(id);
        }

        const insectTime = toTimeSec(currentPhase.insect_time);
        const hasBugs = (plant.insect_owners && plant.insect_owners.length > 0) || (insectTime > 0 && insectTime <= nowSec);
        if (hasBugs) {
            result.needBug.push(id);
        }

        result.growing.push(id);
    }

    return result;
}

function buildLandMap(lands) {
    const map = new Map();
    const list = Array.isArray(lands) ? lands : [];
    for (const land of list) {
        const id = toNum(land && land.id);
        if (id > 0) map.set(id, land);
    }
    return map;
}

function getLandLifecycleState(land) {
    if (!land) return 'unknown';
    const plant = land.plant;
    if (!plant || !Array.isArray(plant.phases) || plant.phases.length === 0) {
        return 'empty';
    }

    const currentPhase = getCurrentPhase(plant.phases, false, '');
    if (!currentPhase) return 'empty';

    const phaseVal = toNum(currentPhase.phase);
    if (phaseVal === PlantPhase.DEAD) return 'dead';
    if (phaseVal === PlantPhase.UNKNOWN) return 'empty';
    if (phaseVal >= PlantPhase.SEED && phaseVal <= PlantPhase.MATURE) return 'growing';
    return 'unknown';
}

function classifyHarvestedLandsByMap(landIds, landsMap) {
    const removable = [];
    const growing = [];
    const unknown = [];
    for (const id of landIds) {
        const land = landsMap.get(id);
        if (!land) {
            unknown.push(id);
            continue;
        }
        const state = getLandLifecycleState(land);
        if (state === 'dead' || state === 'empty') {
            removable.push(id);
            continue;
        }
        if (state === 'growing') {
            growing.push(id);
            continue;
        }
        unknown.push(id);
    }
    return { removable, growing, unknown };
}

async function resolveRemovableHarvestedLands(harvestedLandIds, harvestReply) {
    const ids = Array.isArray(harvestedLandIds) ? harvestedLandIds.filter(Boolean) : [];
    if (ids.length === 0) {
        return { removable: [], growing: [], fallbackRemoved: 0 };
    }

    const replyMap = buildLandMap(harvestReply && harvestReply.land);
    const firstPass = classifyHarvestedLandsByMap(ids, replyMap);
    const removable = [...firstPass.removable];
    const growing = [...firstPass.growing];
    let unknown = [...firstPass.unknown];
    let fallbackRemoved = 0;

    if (unknown.length > 0) {
        try {
            const latestLandsReply = await getAllLands();
            const latestMap = buildLandMap(latestLandsReply && latestLandsReply.lands);
            const secondPass = classifyHarvestedLandsByMap(unknown, latestMap);
            removable.push(...secondPass.removable);
            growing.push(...secondPass.growing);
            unknown = secondPass.unknown;
        } catch (e) {
            logWarn('农场', `收后状态补拉失败: ${e.message}`, {
                module: 'farm',
                event: 'post_harvest_state_fallback',
                result: 'error',
            });
        }
    }

    if (unknown.length > 0) {
        // 按兼容策略：不可判定时保持旧行为，继续铲除
        removable.push(...unknown);
        fallbackRemoved = unknown.length;
    }

    return {
        removable: [...new Set(removable)],
        growing: [...new Set(growing)],
        fallbackRemoved,
    };
}

async function checkFarm() {
    const state = getUserState();
    if (isCheckingFarm || !state.gid || !isAutomationOn('farm')) return false;
    isCheckingFarm = true;

    try {
        // 复用手动操作逻辑
        const result = await runFarmOperation('all', { automated: true });
        isFirstFarmCheck = false;
        return !!(result && result.hadWork);
    } catch (err) {
        logWarn('巡田', `检查失败: ${err.message}`);
        return false;
    } finally {
        isCheckingFarm = false;
    }
}

/**
 * 手动/自动执行农场操作
 * @param {string} opType - 'all', 'harvest', 'clear', 'plant', 'upgrade'
 */
async function runFarmOperation(opType, options = {}) {
    const isAutomated = !!options.automated;
    const landsReply = await getAllLands();
    if (!landsReply.lands || landsReply.lands.length === 0) {
        if (opType !== 'all') {
            log('农场', '没有土地数据');
        }
        return { hadWork: false, actions: [] };
    }

    const lands = landsReply.lands;
    const status = analyzeLands(lands);

    await runOrganicAntiSteal();

    const statusParts = [];
    if (status.harvestable.length) statusParts.push(`收:${status.harvestable.length}`);
    if (status.needWeed.length) statusParts.push(`草:${status.needWeed.length}`);
    if (status.needBug.length) statusParts.push(`虫:${status.needBug.length}`);
    if (status.needWater.length) statusParts.push(`水:${status.needWater.length}`);
    if (status.dead.length) statusParts.push(`枯:${status.dead.length}`);
    if (status.empty.length) statusParts.push(`空:${status.empty.length}`);
    if (status.unlockable.length) statusParts.push(`解:${status.unlockable.length}`);
    if (status.upgradable.length) statusParts.push(`升:${status.upgradable.length}`);
    statusParts.push(`长:${status.growing.length}`);

    const actions = [];
    const optimizer = getFarmOptimizer();

    // 执行除草/虫/水 (使用并发控制)
    if (opType === 'all' || opType === 'clear') {
        const canAutoManageFarm = !isAutomated || !!isAutomationOn('farm_manage');
        const enableAutoWater = !isAutomated || !!isAutomationOn('farm_water');
        const enableAutoWeed = !isAutomated || !!isAutomationOn('farm_weed');
        const enableAutoBug = !isAutomated || !!isAutomationOn('farm_bug');
        const farmOperations = [];
        
        if (canAutoManageFarm && enableAutoWeed && status.needWeed.length > 0) {
            farmOperations.push({
                type: 'weed',
                landIds: status.needWeed,
                fn: async () => {
                    await weedOut(status.needWeed);
                    actions.push(`除草${status.needWeed.length}`);
                    recordOperation('weed', status.needWeed.length);
                }
            });
        }
        if (canAutoManageFarm && enableAutoBug && status.needBug.length > 0) {
            farmOperations.push({
                type: 'bug',
                landIds: status.needBug,
                fn: async () => {
                    await insecticide(status.needBug);
                    actions.push(`除虫${status.needBug.length}`);
                    recordOperation('bug', status.needBug.length);
                }
            });
        }
        if (canAutoManageFarm && enableAutoWater && status.needWater.length > 0) {
            farmOperations.push({
                type: 'water',
                landIds: status.needWater,
                fn: async () => {
                    await waterLand(status.needWater);
                    actions.push(`浇水${status.needWater.length}`);
                    recordOperation('water', status.needWater.length);
                }
            });
        }
        
        // 使用批量操作优化器执行
        if (farmOperations.length > 0) {
            try {
                await optimizer.batchFarmOperations(farmOperations);
            } catch (e) {
                logWarn('农场', `批量操作失败: ${e.message}`);
            }
        }
    }

    // 执行收获
    let harvestedLandIds = [];
    let harvestReply = null;
    if (opType === 'all' || opType === 'harvest') {
        if (status.harvestable.length > 0) {
            try {
                harvestReply = await harvest(status.harvestable);
                log('收获', `收获完成 ${status.harvestable.length} 块土地`, {
                    module: 'farm',
                    event: 'harvest_crop',
                    result: 'ok',
                    count: status.harvestable.length,
                    landIds: [...status.harvestable],
                });
                actions.push(`收获${status.harvestable.length}`);
                recordOperation('harvest', status.harvestable.length);
                harvestedLandIds = [...status.harvestable];
                networkEvents.emit('farmHarvested', {
                    count: status.harvestable.length,
                    landIds: [...status.harvestable],
                    opType,
                });
            } catch (e) {
                logWarn('收获', e.message, {
                    module: 'farm',
                    event: 'harvest_crop',
                    result: 'error',
                });
            }
        }
    }

    // 执行种植
    if (opType === 'all' || opType === 'plant') {
        const allEmptyLands = [...new Set(status.empty)];
        let allDeadLands = [...new Set(status.dead)];

        if (opType === 'all' && harvestedLandIds.length > 0) {
            const postHarvest = await resolveRemovableHarvestedLands(harvestedLandIds, harvestReply);
            allDeadLands = [...new Set([...allDeadLands, ...postHarvest.removable])];
        }
        // 注意：如果是单纯点"一键种植"，harvestedLandIds 为空，只种当前的空地/死地
        if (allDeadLands.length > 0 || allEmptyLands.length > 0) {
            try {
                const plantCount = allDeadLands.length + allEmptyLands.length;
                await autoPlantEmptyLands(allDeadLands, allEmptyLands);
                actions.push(`种植${plantCount}`);
                recordOperation('plant', plantCount);
            } catch (e) { logWarn('种植', e.message); }
        }
    }

    // ==================== 土地解锁/升级逻辑 ====================
    // 判断是否需要执行土地升级操作
    // - 手动操作 (opType === 'upgrade')：总是执行
    // - 自动巡查 (opType === 'all')：受 land_upgrade 开关控制
    const shouldAutoUpgrade = opType === 'all' && isAutomationOn('land_upgrade');
    if (shouldAutoUpgrade || opType === 'upgrade') {
        // ---------- 解锁土地 ----------
        // 检查是否有可解锁的土地
        if (status.unlockable.length > 0) {
            let unlocked = 0;
            // 逐个解锁土地
            for (const landId of status.unlockable) {
                try {
                    // 调用解锁接口，false 表示不使用共享解锁
                    await unlockLand(landId, false);
                    log('解锁', `土地#${landId} 解锁成功`, {
                        module: 'farm', event: 'unlock_land', result: 'ok', landId
                    });
                    unlocked++;
                } catch (e) {
                    logWarn('解锁', `土地#${landId} 解锁失败: ${e.message}`, {
                        module: 'farm', event: 'unlock_land', result: 'error', landId
                    });
                }
                // 每次操作间隔 200ms，避免请求过快
                await sleep(200);
            }
            // 记录解锁成功的数量到操作列表
            if (unlocked > 0) {
                actions.push(`解锁${unlocked}`);
            }
        }

        // ---------- 升级土地 ----------
        // 检查是否有可升级的土地
        if (status.upgradable.length > 0) {
            let upgraded = 0;
            // 逐个升级土地
            for (const landId of status.upgradable) {
                try {
                    // 调用升级接口
                    const reply = await upgradeLand(landId);
                    // 获取升级后的新等级
                    const newLevel = reply.land ? toNum(reply.land.level) : '?';
                    log('升级', `土地#${landId} 升级成功 → 等级${newLevel}`, {
                        module: 'farm', event: 'upgrade_land', result: 'ok', landId, level: newLevel
                    });
                    upgraded++;
                } catch (e) {
                    log('升级', `土地#${landId} 升级失败: ${e.message}`, {
                        module: 'farm', event: 'upgrade_land', result: 'error', landId
                    });
                }
                // 每次操作间隔 200ms，避免请求过快
                await sleep(200);
            }
            // 记录升级成功的数量到操作列表和统计
            if (upgraded > 0) {
                actions.push(`升级${upgraded}`);
                recordOperation('upgrade', upgraded);
            }
        }
    }

    // ==================== 日志输出 ====================
    // 格式化操作结果字符串，例如: " → 收获3/种植5/解锁1"
    const actionStr = actions.length > 0 ? ` → ${actions.join('/')}` : '';
    // 只有执行了操作才输出日志
    if (actions.length > 0) {
         log('农场', `[${statusParts.join(' ')}]${actionStr}`, {
             module: 'farm', event: 'farm_cycle', opType, actions
         });
    }
    // 返回是否有工作执行和操作列表
    return { hadWork: actions.length > 0, actions };
}

/**
 * 调度下一次农场检查
 * 使用定时器在指定延迟后执行下一次农场检查
 * @param {number} delayMs - 延迟时间（毫秒），默认使用配置的检查间隔
 */
function scheduleNextFarmCheck(delayMs = CONFIG.farmCheckInterval) {
    // 如果使用外部调度器，不执行内部调度
    if (externalSchedulerMode) return;
    // 如果农场检查循环未运行，不调度
    if (!farmLoopRunning) return;
    // 设置定时任务，延迟后执行农场检查
    farmScheduler.setTimeoutTask('farm_check_loop', Math.max(0, delayMs), async () => {
        // 再次检查循环是否仍在运行
        if (!farmLoopRunning) return;
        // 执行农场检查
        await checkFarm();
        // 检查完成后，调度下一次检查
        if (!farmLoopRunning) return;
        scheduleNextFarmCheck(CONFIG.farmCheckInterval);
    });
}

/**
 * 启动农场检查循环
 * 开始定期检查农场状态并执行自动化操作
 * @param {object} options - 配置选项
 * @param {boolean} options.externalScheduler - 是否使用外部调度器
 */
function startFarmCheckLoop(options = {}) {
    // 如果循环已经在运行，直接返回
    if (farmLoopRunning) return;
    // 设置是否使用外部调度器模式
    externalSchedulerMode = !!options.externalScheduler;
    // 标记循环为运行状态
    farmLoopRunning = true;
    // 监听土地变化推送事件
    networkEvents.on('landsChanged', onLandsChangedPush);
    // 如果不是外部调度模式，启动内部定时检查
    if (!externalSchedulerMode) {
        // 2秒后开始第一次检查
        scheduleNextFarmCheck(2000);
    }
}

/**
 * 处理土地变化推送事件
 * 当服务器推送土地状态变化时，触发农场检查
 * @param {Array} lands - 变化的土地列表
 */
let lastPushTime = 0;
function onLandsChangedPush(lands) {
    // 检查推送触发巡田开关是否开启
    if (!isAutomationOn('farm_push')) {
        return;
    }
    // 如果正在检查农场，跳过本次推送
    if (isCheckingFarm) return;
    // 防抖：500ms 内只处理一次推送
    const now = Date.now();
    if (now - lastPushTime < 500) return;
    lastPushTime = now;
    // 记录推送日志
    log('农场', `收到推送: ${lands.length}块土地变化，检查中...`, {
        module: 'farm', event: 'lands_notify', result: 'trigger_check', count: lands.length
    });
    // 延迟 100ms 后执行农场检查，避免与正在进行的检查冲突
    farmScheduler.setTimeoutTask('farm_push_check', 100, async () => {
        if (!isCheckingFarm) await checkFarm();
    });
}

/**
 * 停止农场检查循环
 * 清理所有定时任务和事件监听
 */
function stopFarmCheckLoop() {
    // 标记循环为停止状态
    farmLoopRunning = false;
    // 重置外部调度器模式
    externalSchedulerMode = false;
    // 清理所有定时任务
    farmScheduler.clearAll();
    // 移除土地变化推送事件监听
    networkEvents.removeListener('landsChanged', onLandsChangedPush);
}

/**
 * 刷新农场检查循环
 * 立即重新调度下一次农场检查
 * @param {number} delayMs - 延迟时间（毫秒），默认 200ms
 */
function refreshFarmCheckLoop(delayMs = 200) {
    // 如果循环未运行，不执行
    if (!farmLoopRunning) return;
    // 重新调度下一次检查
    scheduleNextFarmCheck(delayMs);
}

module.exports = {
    checkFarm, startFarmCheckLoop, stopFarmCheckLoop,
    refreshFarmCheckLoop,
    getCurrentPhase,
    setOperationLimitsCallback,
    getAllLands,
    getLandsDetail,
    getAvailableSeeds,
    runFarmOperation,
    runFertilizerByConfig,
    runOrganicAntiSteal,
};
