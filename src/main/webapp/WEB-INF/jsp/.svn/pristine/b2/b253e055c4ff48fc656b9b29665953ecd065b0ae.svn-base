<?php

/**
 * This is the model class for table "ad_stats_interest".
 *
 * The followings are the available columns in table 'ad_stats_interest':
 * @property string $id
 * @property integer $ad_group_id
 * @property integer $ad_plan_id
 * @property integer $ad_user_id
 * @property string $inter_id
 * @property integer $clicks
 * @property integer $views
 * @property string $costs
 * @property integer $trans
 * @property string $create_date
 */
class AdInterest extends CActiveRecord
{
    static $db;
	/**
	 * Returns the static model of the specified AR class.
	 * @return AdInterest the static model class
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
		return 'ad_stats_interest';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('ad_group_id, ad_plan_id, ad_user_id, inter_id, clicks, views, costs, create_date', 'required'),
			array('ad_group_id, ad_plan_id, ad_user_id, clicks, views, trans', 'numerical', 'integerOnly'=>true),
			array('inter_id', 'length', 'max'=>32),
			array('costs', 'length', 'max'=>10),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, ad_group_id, ad_plan_id, ad_user_id, inter_id, clicks, views, costs, trans, create_date', 'safe', 'on'=>'search'),
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
			'ad_group_id' => 'Ad Group',
			'ad_plan_id' => 'Ad Plan',
			'ad_user_id' => 'Ad User',
			'inter_id' => 'Inter',
			'clicks' => 'Clicks',
			'views' => 'Views',
			'costs' => 'Costs',
			'trans' => 'Trans',
			'create_date' => 'Create Date',
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

		$criteria->compare('ad_group_id',$this->ad_group_id);

		$criteria->compare('ad_plan_id',$this->ad_plan_id);

		$criteria->compare('ad_user_id',$this->ad_user_id);

		$criteria->compare('inter_id',$this->inter_id,true);

		$criteria->compare('clicks',$this->clicks);

		$criteria->compare('views',$this->views);

		$criteria->compare('costs',$this->costs,true);

		$criteria->compare('trans',$this->trans);

		$criteria->compare('create_date',$this->create_date,true);

		return new CActiveDataProvider('AdInterest', array(
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
}
