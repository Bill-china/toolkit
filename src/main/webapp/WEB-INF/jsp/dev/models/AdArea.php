<?php

/**
 * This is the model class for table "ad_stats_area".
 *
 * The followings are the available columns in table 'ad_stats_area':
 * @property string $id
 * @property integer $area_id
 * @property integer $area_fid
 * @property integer $ad_group_id
 * @property integer $ad_plan_id
 * @property integer $ad_user_id
 * @property integer $clicks
 * @property integer $views
 * @property string $costs
 * @property integer $trans
 * @property string $create_date
 * @property string $area_key
 * @property string $update_time
 */
class AdArea extends CActiveRecord
{
    static $db;
	/**
	 * Returns the static model of the specified AR class.
	 * @return AdArea the static model class
	 */
	public static function model($className=__CLASS__)
	{
		return parent::model($className);
	}

	/**
	 * @return string the associated database table name
	 */
	public function tableName()
	{
		return 'ad_stats_area';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('area_id, area_fid, ad_group_id, ad_user_id, clicks, views, costs', 'required'),
			array('area_id, area_fid, ad_group_id, ad_plan_id, ad_user_id, clicks, views, trans', 'numerical', 'integerOnly'=>true),
			array('costs', 'length', 'max'=>10),
			array('area_key', 'length', 'max'=>32),
			array('update_time', 'safe'),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, area_id, area_fid, ad_group_id, ad_plan_id, ad_user_id, clicks, views, costs, trans, create_date, area_key, update_time', 'safe', 'on'=>'search'),
		);
	}

	/**
	 * @return array relational rules.
	 */
	public function relations()
	{
		// NOTE: you may need to adjust the relation name and the related
		// class name for the relations automatically generated below.
		return array(
		);
	}

	/**
	 * @return array customized attribute labels (name=>label)
	 */
	public function attributeLabels()
	{
		return array(
			'id' => 'Id',
			'area_id' => 'Area',
			'area_fid' => 'Area Fid',
			'ad_group_id' => 'Ad Group',
			'ad_plan_id' => 'Ad Plan',
			'ad_user_id' => 'Ad User',
			'clicks' => 'Clicks',
			'views' => 'Views',
			'costs' => 'Costs',
			'trans' => 'Trans',
			'create_date' => 'Create Date',
			'area_key' => 'Area Key',
			'update_time' => 'Update Time',
		);
	}

	/**
	 * Retrieves a list of models based on the current search/filter conditions.
	 * @return CActiveDataProvider the data provider that can return the models based on the search/filter conditions.
	 */
	public function search()
	{
		// Warning: Please modify the following code to remove attributes that
		// should not be searched.

		$criteria=new CDbCriteria;

		$criteria->compare('id',$this->id,true);

		$criteria->compare('area_id',$this->area_id);

		$criteria->compare('area_fid',$this->area_fid);

		$criteria->compare('ad_group_id',$this->ad_group_id);

		$criteria->compare('ad_plan_id',$this->ad_plan_id);

		$criteria->compare('ad_user_id',$this->ad_user_id);

		$criteria->compare('clicks',$this->clicks);

		$criteria->compare('views',$this->views);

		$criteria->compare('costs',$this->costs,true);

		$criteria->compare('trans',$this->trans);

		$criteria->compare('create_date',$this->create_date,true);

		$criteria->compare('area_key',$this->area_key,true);

		$criteria->compare('update_time',$this->update_time,true);

		return new CActiveDataProvider('AdArea', array(
			'criteria'=>$criteria,
		));
	}

    public function getDbConnection()
    {
        if(self::$db!==null)
            return self::$db;
        else{
            self::$db=Yii::app()->db_stats;
            if(self::$db instanceof CDbConnection)
            {
                self::$db->setActive(true);
                return self::$db;
            }
            else
                throw new CDbException(Yii::t('yii','...'));
        }
    }

    public function getGroupData($date)
    {
        $sql = "select ad_group_id, sum(costs) as sum_total_cost, sum(clicks) as sum_clicks, sum(views) as sum_views, sum(trans) as sum_trans from " . $this->tableName() . " where create_date = :date group by ad_group_id";
        $cmd = Yii::app()->db_stats->createCommand($sql);
        $cmd->bindParam(':date', $date, PDO::PARAM_STR);
        $arr = array();
        if($rows = $cmd->queryAll()){
            foreach($rows as $value){
                $arr[$value['ad_group_id']] = $value;
                unset($arr[$value['ad_group_id']]['ad_group_id']);
            }
        }
        return $arr;
    }

    //通过groupId, create_date取一下是否有area为其它地区的记录，如果没有，补充一条，用来补数据。
    public function getOtherAreaByGroupId($groupId, $date)
    {
        $sql = 'select id, clicks, views, costs, trans from ' . $this->tableName() . ' where create_date=:date and ad_group_id=:groupId and area_fid = 0 and area_id = 0 limit 1';
        $cmd = Yii::app()->db_stats->createCommand($sql);
        $cmd->bindParam(':date', $date, PDO::PARAM_STR);
        $cmd->bindParam(':groupId', $groupId, PDO::PARAM_INT);
        return $cmd->queryRow();
    }

    //从area去数据的时候，万一other区域不够，还需要从正常区域del
    public function getCommonArea($date, $groupId, $field, $value)
    {
        $sql = "update " . $this->tableName() . " set $field = $field + $value where create_date=:date and ad_group_id=:groupId and $field >= $value * -1 limit 1";
        $cmd = Yii::app()->db_stats->createCommand($sql);
        $cmd->bindParam(':date', $date, PDO::PARAM_STR);
        $cmd->bindParam(':groupId', $groupId, PDO::PARAM_INT);
        if($rowCount = $cmd->execute()){
            return true;
        }else
            return false;

    }
}
