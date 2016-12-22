<?php

/**
 * This is the model class for table "stats".
 *
 * The followings are the available columns in table 'stats':
 * @property string $id
 * @property string $time
 * @property string $key
 * @property integer $advert_id
 * @property integer $plan_id
 * @property integer $group_id
 * @property integer $user_id
 * @property string $stats_date
 * @property integer $last_update_time
 * @property integer $view_times
 * @property integer $click_times
 * @property integer $trans_times
 * @property double $total_cost
 * @property integer $status
 */
class Stats extends CActiveRecord
{
    static $db;
	/**
	 * Returns the static model of the specified AR class.
	 * @return Stats the static model class
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
		return 'stats';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('time, key, advert_id, plan_id, group_id, user_id, stats_date, last_update_time, view_times, click_times, trans_times, total_cost', 'required'),
			array('advert_id, plan_id, group_id, user_id, last_update_time, view_times, click_times, trans_times, status', 'numerical', 'integerOnly'=>true),
			array('total_cost', 'numerical'),
			array('key', 'length', 'max'=>128),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, time, key, advert_id, plan_id, group_id, user_id, stats_date, last_update_time, view_times, click_times, trans_times, total_cost, status', 'safe', 'on'=>'search'),
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
			'time' => 'Time',
			'key' => 'Key',
			'advert_id' => 'Advert',
			'plan_id' => 'Plan',
			'group_id' => 'Group',
			'user_id' => 'User',
			'stats_date' => 'Stats Date',
			'last_update_time' => 'Last Update Time',
			'view_times' => 'View Times',
			'click_times' => 'Click Times',
			'trans_times' => 'Trans Times',
			'total_cost' => 'Total Cost',
			'status' => 'Status',
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

		$criteria->compare('time',$this->time,true);

		$criteria->compare('key',$this->key,true);

		$criteria->compare('advert_id',$this->advert_id);

		$criteria->compare('plan_id',$this->plan_id);

		$criteria->compare('group_id',$this->group_id);

		$criteria->compare('user_id',$this->user_id);

		$criteria->compare('stats_date',$this->stats_date,true);

		$criteria->compare('last_update_time',$this->last_update_time);

		$criteria->compare('view_times',$this->view_times);

		$criteria->compare('click_times',$this->click_times);

		$criteria->compare('trans_times',$this->trans_times);

		$criteria->compare('total_cost',$this->total_cost);

		$criteria->compare('status',$this->status);

		return new CActiveDataProvider('Stats', array(
			'criteria'=>$criteria,
		));
	}

    public function getDbConnection()
    {
        if(self::$db!==null)
            return self::$db;
        else{
            self::$db=Yii::app()->db;
            if(self::$db instanceof CDbConnection)
            {
                self::$db->setActive(true);
                return self::$db;
            }
            else
                throw new CDbException(Yii::t('yii','...'));
        }
    }

    public function getMaxId($date = null){
        if($date){
            $sql = "select max(id) from " . $this->tableName() . " where stats_date = :date and status = 0";
            $cmd = Yii::app()->db->createCommand($sql);
            $cmd->bindParam(':date', $date, PDO::PARAM_STR);
        }else{
            $sql = "select max(id) from " . $this->tableName() . " where status = 0";
            $cmd = Yii::app()->db->createCommand($sql);
        }
        if($maxId = $cmd->queryScalar())
            return $maxId;
        else
            return 0;
    }

    public function getMinId($date = null){
        if($date){
            $sql = "select min(id) from " . $this->tableName() . " where stats_date = :date and status = 0";
            $cmd = Yii::app()->db->createCommand($sql);
            $cmd->bindParam(':date', $date, PDO::PARAM_STR);
        }else{
            $sql = "select min(id) from " . $this->tableName() . " where status = 0";
            $cmd = Yii::app()->db->createCommand($sql);
        }
        if($maxId = $cmd->queryScalar())
            return $maxId;
        else
            return 0;
    }
}
