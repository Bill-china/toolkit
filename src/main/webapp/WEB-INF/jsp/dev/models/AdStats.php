<?php

/**
 * This is the model class for table "ad_stats".
 *
 * The followings are the available columns in table 'ad_stats':
 * @property integer $id
 * @property integer $clicks
 * @property integer $views
 * @property string $total_cost
 * @property integer $trans
 * @property integer $status
 * @property string $create_date
 * @property integer $ad_group_id
 * @property integer $ad_plan_id
 * @property integer $ad_advert_id
 * @property integer $ad_user_id
 * @property integer $ad_channel_id
 * @property integer $ad_place_id
 * @property integer $admin_user_id
 * @property integer $last_update_time
 * @property integer $data_source
 */
class AdStats extends CActiveRecord
{
    static $db;
    /**
     * Returns the static model of the specified AR class.
     * @return AdStats the static model class
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
        return 'ad_stats';
    }

    /**
     * @return array validation rules for model attributes.
     */
    public function rules()
    {
        // NOTE: you should only define rules for those attributes that
        // will receive user inputs.
        return array(
            array('create_date, ad_group_id, ad_plan_id, ad_advert_id, ad_user_id, last_update_time', 'required'),
            array('clicks, views, trans, status, ad_group_id, ad_plan_id, ad_advert_id, ad_user_id, ad_channel_id, ad_place_id, admin_user_id, last_update_time, data_source', 'numerical', 'integerOnly'=>true),
            array('total_cost', 'length', 'max'=>10),
            // The following rule is used by search().
            // Please remove those attributes that should not be searched.
            array('id, clicks, views, total_cost, trans, status, create_date, ad_group_id, ad_plan_id, ad_advert_id, ad_user_id, ad_channel_id, ad_place_id, admin_user_id, last_update_time, data_source', 'safe', 'on'=>'search'),
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
            'clicks' => 'Clicks',
            'views' => 'Views',
            'total_cost' => 'Total Cost',
            'trans' => 'Trans',
            'status' => 'Status',
            'create_date' => 'Create Date',
            'ad_group_id' => 'Ad Group',
            'ad_plan_id' => 'Ad Plan',
            'ad_advert_id' => 'Ad Advert',
            'ad_user_id' => 'Ad User',
            'ad_channel_id' => 'Ad Channel',
            'ad_place_id' => 'Ad Place',
            'admin_user_id' => 'Admin User',
            'last_update_time' => 'Last Update Time',
            'data_source' => 'Data Source',
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

        $criteria->compare('id',$this->id);

        $criteria->compare('clicks',$this->clicks);

        $criteria->compare('views',$this->views);

        $criteria->compare('total_cost',$this->total_cost,true);

        $criteria->compare('trans',$this->trans);

        $criteria->compare('status',$this->status);

        $criteria->compare('create_date',$this->create_date,true);

        $criteria->compare('ad_group_id',$this->ad_group_id);

        $criteria->compare('ad_plan_id',$this->ad_plan_id);

        $criteria->compare('ad_advert_id',$this->ad_advert_id);

        $criteria->compare('ad_user_id',$this->ad_user_id);

        $criteria->compare('ad_channel_id',$this->ad_channel_id);

        $criteria->compare('ad_place_id',$this->ad_place_id);

        $criteria->compare('admin_user_id',$this->admin_user_id);

        $criteria->compare('last_update_time',$this->last_update_time);

        $criteria->compare('data_source',$this->data_source);

        return new CActiveDataProvider('AdStats', array(
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

    public function getGroupInfo($groupId)
    {
        $sql = "select ad_plan_id, ad_user_id from " . $this->tableName() . " where ad_group_id = :groupId limit 1";
        $cmd = Yii::app()->db_stats->createCommand($sql);
        $cmd->bindParam(':groupId', $groupId, PDO::PARAM_INT);
        if($row = $cmd->queryRow())
            return $row;
        else
            return false;
    }

    public function getGroupData($date)
    {
        $sql = "select ad_group_id, sum(total_cost) as sum_total_cost, sum(clicks) as sum_clicks, sum(views) as sum_views, sum(trans) as sum_trans from " . $this->tableName() . " where status = 0 and create_date = :date group by ad_group_id";
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
}
