<?php

/**
 * This is the model class for table "keyword".
 *
 * The followings are the available columns in table 'keyword':
 * @property string $id
 * @property string $time
 * @property string $key
 * @property integer $group_id
 * @property integer $user_id
 * @property integer $plan_id
 * @property string $keyword
 * @property integer $click_times
 * @property double $view_times
 * @property double $total_cost
 * @property integer $trans_times
 * @property string $stats_date
 * @property integer $status
 */
class Keyword extends CActiveRecord
{
    static $db;
	/**
	 * Returns the static model of the specified AR class.
	 * @return Keyword the static model class
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
		return 'keyword';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('time, key, group_id, user_id, plan_id, keyword, click_times, view_times, total_cost, trans_times, stats_date', 'required'),
			array('group_id, user_id, plan_id, click_times, trans_times, status', 'numerical', 'integerOnly'=>true),
			array('view_times, total_cost', 'numerical'),
			array('key', 'length', 'max'=>64),
			array('keyword', 'length', 'max'=>128),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, time, key, group_id, user_id, plan_id, keyword, click_times, view_times, total_cost, trans_times, stats_date, status', 'safe', 'on'=>'search'),
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
			'group_id' => 'Group',
			'user_id' => 'User',
			'plan_id' => 'Plan',
			'keyword' => 'Keyword',
			'click_times' => 'Click Times',
			'view_times' => 'View Times',
			'total_cost' => 'Total Cost',
			'trans_times' => 'Trans Times',
			'stats_date' => 'Stats Date',
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

		$criteria->compare('group_id',$this->group_id);

		$criteria->compare('user_id',$this->user_id);

		$criteria->compare('plan_id',$this->plan_id);

		$criteria->compare('keyword',$this->keyword,true);

		$criteria->compare('click_times',$this->click_times);

		$criteria->compare('view_times',$this->view_times);

		$criteria->compare('total_cost',$this->total_cost);

		$criteria->compare('trans_times',$this->trans_times);

		$criteria->compare('stats_date',$this->stats_date,true);

		$criteria->compare('status',$this->status);

		return new CActiveDataProvider('Keyword', array(
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
